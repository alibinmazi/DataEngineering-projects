"""
=============================================================================
UEH MVP — DAG 1: NVD Raw Ingestion
=============================================================================
What it does:
    1. Check adapter is active (pre-flight)
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
ADAPTER_INSTANCE_ID = "nvd_public_01"

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
    - adapter_state.state_status != 'FAILING' (circuit breaker)
    - No existing RAW_COMPLETE for today (avoid duplicate)
    
    Returns True to proceed, False to skip.
    """
    spark = get_spark()
    today = kwargs['ds']  # YYYY-MM-DD

    # Is adapter active?
    config = spark.sql(f"""
        SELECT is_active FROM ueh_dev_control.t01_ueh_ctl_adapter_config
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
    """).first()

    if not config or not config.is_active:
        logger.warning(f"Adapter {ADAPTER_INSTANCE_ID} is INACTIVE. Skipping.")
        return False

    # Is adapter healthy enough?
    state = spark.sql(f"""
        SELECT state_status, consecutive_failures
        FROM ueh_dev_control.t01_ueh_ctl_adapter_state
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
    """).first()

    if state and state.consecutive_failures >= 5:
        logger.error(f"Adapter has {state.consecutive_failures} consecutive failures. SKIPPING.")
        return False

    # Already ran today?
    existing = spark.sql(f"""
        SELECT batch_id FROM ueh_dev_control.t01_ueh_ctl_batch_registry
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
          AND ingestion_date = '{today}'
          AND status IN ('RAW_COMPLETE', 'BRONZE_COMPLETE')
    """).first()

    if existing:
        logger.info(f"Already have batch for {today}: {existing.batch_id}. Skipping.")
        return False

    logger.info("Pre-flight PASSED. Proceeding with ingestion.")
    return True


# ─── Task 2: Trigger NiFi ────────────────────────────────────────────────────
def trigger_nifi(**kwargs):
    """
    Start the NiFi NVD ingestion process group via REST API.
    
    NOTE: Replace with your actual NiFi URL and process group ID.
    If NiFi is triggered differently in your setup (e.g., CRON within NiFi),
    this task can simply be a log/no-op.
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
        logger.warning(f"NiFi trigger failed: {e}. Flow may be self-scheduled.")
        # Don't fail the task — NiFi might be self-triggered via cron

    kwargs['ti'].xcom_push(key='trigger_time', value=datetime.utcnow().isoformat())


# ─── Task 3: Wait for RAW_COMPLETE ───────────────────────────────────────────
def wait_for_raw_complete(**kwargs):
    """
    Poll batch_registry every 60 seconds until RAW_COMPLETE appears.
    
    This is the UEH pattern:
    - Do NOT use FileSensor (doesn't understand business status)
    - DO poll control table (understands pipeline lifecycle)
    
    Timeout after 60 minutes (SLA).
    """
    spark = get_spark()
    today = kwargs['ds']
    
    poll_interval = 60      # seconds
    max_wait = 60 * 60      # 60 minutes
    elapsed = 0

    logger.info(f"Polling for RAW_COMPLETE: adapter={ADAPTER_INSTANCE_ID}, date={today}")

    while elapsed < max_wait:
        result = spark.sql(f"""
            SELECT batch_id, records_ingested, bronze_path
            FROM ueh_dev_control.t01_ueh_ctl_batch_registry
            WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
              AND ingestion_date = '{today}'
              AND status = 'RAW_COMPLETE'
            ORDER BY created_at DESC
            LIMIT 1
        """).first()

        if result:
            logger.info(f"RAW_COMPLETE! batch={result.batch_id}, records={result.records_ingested}")
            kwargs['ti'].xcom_push(key='batch_id', value=result.batch_id)
            kwargs['ti'].xcom_push(key='bronze_path', value=result.bronze_path)
            return

        # Check for failure
        failed = spark.sql(f"""
            SELECT batch_id, failure_reason
            FROM ueh_dev_control.t01_ueh_ctl_batch_registry
            WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
              AND ingestion_date = '{today}'
              AND status = 'FAILED'
            ORDER BY created_at DESC LIMIT 1
        """).first()

        if failed:
            raise Exception(f"Ingestion FAILED: {failed.batch_id} — {failed.failure_reason}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise Exception(f"TIMEOUT: RAW_COMPLETE not achieved within {max_wait // 60} minutes.")


# ─── Task 4: Log Success ─────────────────────────────────────────────────────
def log_success(**kwargs):
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')
    logger.info(f"DAG 1 COMPLETE. Batch {batch_id} is RAW_COMPLETE. DAG 2 will pick it up.")


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
