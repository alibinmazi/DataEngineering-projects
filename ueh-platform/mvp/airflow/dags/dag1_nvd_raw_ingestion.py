"""
=============================================================================
UEH MVP — DAG 1: NVD Raw Ingestion
=============================================================================
Database: t01_ueh_dev_ctl
Tables:
  - t01_ueh_ctl_adapter_config (read: is_active, schedule_enabled)
  - t01_ueh_ctl_adapter_state (read: state_status, consecutive_failures)
  - t01_ueh_ctl_batch_registry (poll: batch_status = 'RAW_COMPLETE')

What it does:
    1. Pre-flight: check adapter is active + healthy
    2. Trigger NiFi NVD ingestion flow
    3. Poll batch_registry for RAW_COMPLETE
    4. Log success

Schedule: Daily at 03:00 UTC
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import time
import logging
import requests

logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
DAG_ID = "ueh_dag1_nvd_raw_ingestion"
ADAPTER_INSTANCE_ID = "nvd_prod_01"
ORG_ID = "default_org"
DB_CONTROL = "t01_ueh_dev_ctl"

default_args = {
    'owner': 'ueh',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# ─── Helper ──────────────────────────────────────────────────────────────────
def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName(f"UEH_DAG1_{ADAPTER_INSTANCE_ID}") \
        .enableHiveSupport() \
        .getOrCreate()


# ─── Task 1: Pre-flight Check ────────────────────────────────────────────────
def preflight_check(**kwargs):
    """
    Check:
    - adapter_config.is_active = TRUE
    - adapter_config.schedule_enabled = TRUE
    - adapter_state.state_status != 'FAILING'
    - No existing RAW_COMPLETE/BRONZE_COMPLETE for today

    Returns True to proceed, False to skip.
    """
    spark = get_spark()
    today = kwargs['ds']  # YYYY-MM-DD

    # Is adapter active and schedule enabled?
    config = spark.sql(f"""
        SELECT is_active, schedule_enabled
        FROM {DB_CONTROL}.t01_ueh_ctl_adapter_config
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
          AND org_id = '{ORG_ID}'
    """).first()

    if not config:
        logger.error(f"Adapter {ADAPTER_INSTANCE_ID} not found in adapter_config!")
        return False

    if not config.is_active:
        logger.warning(f"Adapter {ADAPTER_INSTANCE_ID} is INACTIVE. Skipping.")
        return False

    if config.schedule_enabled is False:
        logger.warning(f"Adapter {ADAPTER_INSTANCE_ID} schedule is DISABLED. Skipping.")
        return False

    # Is adapter healthy enough?
    state = spark.sql(f"""
        SELECT state_status, consecutive_failures
        FROM {DB_CONTROL}.t01_ueh_ctl_adapter_state
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
          AND org_id = '{ORG_ID}'
    """).first()

    if state and state.state_status == 'FAILING':
        logger.error(
            f"Adapter {ADAPTER_INSTANCE_ID} is FAILING "
            f"(consecutive_failures={state.consecutive_failures}). "
            f"Manual intervention required. SKIPPING."
        )
        return False

    if state and state.state_status == 'DISABLED':
        logger.warning(f"Adapter {ADAPTER_INSTANCE_ID} is DISABLED. Skipping.")
        return False

    # Already ran today?
    existing = spark.sql(f"""
        SELECT batch_id, batch_status
        FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
          AND org_id = '{ORG_ID}'
          AND ingestion_date = DATE '{today}'
          AND batch_status IN ('RAW_COMPLETE', 'BRONZE_COMPLETE')
    """).first()

    if existing:
        logger.info(
            f"Already have batch for {today}: {existing.batch_id} "
            f"(status={existing.batch_status}). Skipping duplicate."
        )
        return False

    logger.info("Pre-flight PASSED. Proceeding with ingestion.")
    return True


# ─── Task 2: Trigger NiFi ────────────────────────────────────────────────────
def trigger_nifi(**kwargs):
    """
    Start the NiFi NVD ingestion process group via REST API.

    NOTE: If NiFi is self-scheduled (cron within NiFi), this task
    can be a no-op / just log that we expect NiFi to run.
    """
    nifi_url = Variable.get("ueh_nifi_base_url", default_var="http://nifi:8080")
    pg_id = Variable.get("ueh_nifi_pg_nvd", default_var="your-pg-id")

    try:
        response = requests.put(
            f"{nifi_url}/nifi-api/flow/process-groups/{pg_id}",
            json={"id": pg_id, "state": "RUNNING"},
            timeout=30
        )
        response.raise_for_status()
        logger.info(f"NiFi process group {pg_id} triggered successfully.")
    except Exception as e:
        logger.warning(
            f"NiFi trigger attempt: {e}. "
            f"Flow may be self-scheduled — continuing to poll."
        )

    kwargs['ti'].xcom_push(key='trigger_time', value=datetime.utcnow().isoformat())


# ─── Task 3: Wait for RAW_COMPLETE ───────────────────────────────────────────
def wait_for_raw_complete(**kwargs):
    """
    Poll t01_ueh_ctl_batch_registry every 60 seconds
    until batch_status = 'RAW_COMPLETE' appears.

    This is the UEH orchestration pattern:
    - Do NOT use FileSensor
    - DO poll control table for business-level status

    Timeout after 60 minutes (SLA).
    """
    spark = get_spark()
    today = kwargs['ds']

    poll_interval = 60      # seconds
    max_wait = 60 * 60      # 60 minutes
    elapsed = 0

    logger.info(
        f"Polling for RAW_COMPLETE: "
        f"adapter={ADAPTER_INSTANCE_ID}, date={today}"
    )

    while elapsed < max_wait:
        result = spark.sql(f"""
            SELECT batch_id, records_expected, bronze_path
            FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
            WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
              AND org_id = '{ORG_ID}'
              AND ingestion_date = DATE '{today}'
              AND batch_status = 'RAW_COMPLETE'
            ORDER BY created_at DESC
            LIMIT 1
        """).first()

        if result:
            logger.info(
                f"RAW_COMPLETE detected! "
                f"batch={result.batch_id}, "
                f"records={result.records_expected}, "
                f"path={result.bronze_path}"
            )
            kwargs['ti'].xcom_push(key='batch_id', value=result.batch_id)
            kwargs['ti'].xcom_push(key='bronze_path', value=result.bronze_path)
            return

        # Check for FAILED status
        failed = spark.sql(f"""
            SELECT batch_id, failure_reason, failure_category
            FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
            WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
              AND org_id = '{ORG_ID}'
              AND ingestion_date = DATE '{today}'
              AND batch_status = 'FAILED'
            ORDER BY created_at DESC LIMIT 1
        """).first()

        if failed:
            raise Exception(
                f"Ingestion FAILED: batch={failed.batch_id}, "
                f"category={failed.failure_category}, "
                f"reason={failed.failure_reason}"
            )

        time.sleep(poll_interval)
        elapsed += poll_interval

        if elapsed % 300 == 0:
            logger.info(f"Still waiting... ({elapsed}s / {max_wait}s)")

    raise Exception(
        f"TIMEOUT: RAW_COMPLETE not achieved within {max_wait // 60} minutes. "
        f"SLA BREACH for {ADAPTER_INSTANCE_ID}."
    )


# ─── Task 4: Log Success ─────────────────────────────────────────────────────
def log_success(**kwargs):
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')
    logger.info(
        f"DAG 1 COMPLETE. Batch {batch_id} is RAW_COMPLETE. "
        f"DAG 2 will pick it up for Bronze loading."
    )


# ─── DAG Definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH DAG 1: Trigger NVD ingestion, wait for RAW_COMPLETE',
    schedule_interval='0 3 * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'nvd', 'dag1', 'ingestion'],
) as dag:

    t1_preflight = ShortCircuitOperator(
        task_id='preflight_check',
        python_callable=preflight_check,
    )

    t2_trigger = PythonOperator(
        task_id='trigger_nifi',
        python_callable=trigger_nifi,
    )

    t3_wait = PythonOperator(
        task_id='wait_for_raw_complete',
        python_callable=wait_for_raw_complete,
        execution_timeout=timedelta(minutes=90),
    )

    t4_done = PythonOperator(
        task_id='log_success',
        python_callable=log_success,
    )

    t1_preflight >> t2_trigger >> t3_wait >> t4_done
