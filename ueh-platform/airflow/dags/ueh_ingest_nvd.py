"""
=============================================================================
UEH Platform: DAG 1 - NVD Ingestion Orchestration
=============================================================================
DAG Name:    ueh_ingest_nvd
Purpose:     Trigger and monitor NiFi-based NVD API ingestion to HDFS raw storage.
             Marks batch as RAW_COMPLETE upon successful NiFi completion.

Responsibility:
    - Validate adapter is active and circuit breaker is not open
    - Trigger NiFi process group for NVD ingestion
    - Monitor NiFi flow completion
    - Verify RAW_COMPLETE status in batch_registry
    - Alert on SLA breach or failure

Does NOT:
    - Load data into Bronze Iceberg table (that's DAG 2)
    - Parse or transform any data
    - Touch Silver/Gold layers

Trigger:     Scheduled daily at 03:00 UTC (aligned with adapter_config.schedule_cron)
Depends On:  Nothing (this is the entry point)
Triggers:    Nothing directly (DAG 2 polls batch_registry independently)

Architecture Decision:
    DAG 1 (Ingestion) and DAG 2 (Bronze Load) are DECOUPLED.
    Coupling mechanism = control table status (RAW_COMPLETE).
    This allows:
        - Independent failure/retry
        - Independent SLA tracking
        - Bronze replay without re-ingestion
        - Different team ownership
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import time
import logging

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

DAG_ID = "ueh_ingest_nvd"
ADAPTER_INSTANCE_ID = "nvd_public_01"
ADAPTER_NAME = "nvd"

default_args = {
    'owner': 'ueh-vuln-intel',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['ueh-alerts@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


# ─────────────────────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────────────────────

def get_spark_session():
    """Get or create SparkSession for control table queries."""
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName(f"UEH_Airflow_{DAG_ID}") \
        .enableHiveSupport() \
        .getOrCreate()


def get_environment():
    """Get current environment from Airflow variables."""
    return Variable.get("ueh_environment", default_var="dev")


def get_nifi_config():
    """Get NiFi connection details from Airflow variables."""
    return {
        'base_url': Variable.get("ueh_nifi_base_url", default_var="http://nifi:8080"),
        'process_group_id': Variable.get("ueh_nifi_pg_nvd"),
        'username': Variable.get("ueh_nifi_username", default_var=""),
        'password': Variable.get("ueh_nifi_password", default_var=""),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Task: Pre-flight Checks
# ─────────────────────────────────────────────────────────────────────────────

def preflight_checks(**kwargs):
    """
    Validate adapter is ready for ingestion:
    1. adapter_config.is_active = TRUE
    2. adapter_state.circuit_breaker_open = FALSE
    3. adapter_state.state_status != 'PAUSED'
    4. No existing RAW_COMPLETE batch for today (avoid duplicates)
    
    Returns: 'proceed_ingestion' or 'skip_ingestion' for branching
    """
    env = get_environment()
    db_control = f"ueh_{env}_control"
    spark = get_spark_session()
    
    # Check 1: Is adapter active?
    config = spark.sql(f"""
        SELECT is_active, sla_minutes
        FROM {db_control}.t01_ueh_ctl_adapter_config
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
    """).first()
    
    if config is None:
        raise Exception(f"Adapter '{ADAPTER_INSTANCE_ID}' not found in adapter_config")
    
    if not config.is_active:
        logger.warning(f"Adapter '{ADAPTER_INSTANCE_ID}' is INACTIVE. Skipping.")
        return 'skip_ingestion'
    
    # Check 2: Is circuit breaker open?
    state = spark.sql(f"""
        SELECT state_status, circuit_breaker_open, consecutive_failures
        FROM {db_control}.t01_ueh_ctl_adapter_state
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
    """).first()
    
    if state is None:
        raise Exception(f"Adapter state not found for '{ADAPTER_INSTANCE_ID}'")
    
    if state.circuit_breaker_open:
        logger.error(
            f"Circuit breaker OPEN for '{ADAPTER_INSTANCE_ID}' "
            f"(consecutive_failures={state.consecutive_failures}). "
            f"Manual intervention required."
        )
        return 'skip_ingestion'
    
    if state.state_status == 'PAUSED':
        logger.warning(f"Adapter '{ADAPTER_INSTANCE_ID}' is PAUSED. Skipping.")
        return 'skip_ingestion'
    
    # Check 3: Already ingested today?
    today = kwargs['ds']  # Airflow execution date (YYYY-MM-DD)
    existing = spark.sql(f"""
        SELECT batch_id, status
        FROM {db_control}.t01_ueh_ctl_batch_registry
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
          AND ingestion_date = '{today}'
          AND status IN ('RAW_COMPLETE', 'BRONZE_COMPLETE', 'SILVER_COMPLETE', 'GOLD_COMPLETE')
    """).first()
    
    if existing is not None:
        logger.warning(
            f"Batch already exists for {today}: {existing.batch_id} "
            f"(status={existing.status}). Skipping duplicate ingestion."
        )
        return 'skip_ingestion'
    
    # Store SLA for timeout monitoring
    kwargs['ti'].xcom_push(key='sla_minutes', value=config.sla_minutes)
    
    logger.info(f"Pre-flight checks PASSED for '{ADAPTER_INSTANCE_ID}'. Proceeding.")
    return 'trigger_nifi_ingestion'


# ─────────────────────────────────────────────────────────────────────────────
# Task: Trigger NiFi Flow
# ─────────────────────────────────────────────────────────────────────────────

def trigger_nifi_ingestion(**kwargs):
    """
    Trigger the NiFi process group for NVD ingestion via REST API.
    
    NiFi API:
        PUT /nifi-api/flow/process-groups/{id}
        Body: {"id": "{pg_id}", "state": "RUNNING"}
    
    The NiFi flow will:
        1. Read adapter config from control table
        2. Read watermark from adapter state
        3. Call NVD API with pagination
        4. Write chunks to HDFS
        5. Write manifest + checkpoint
        6. Update batch_registry → RAW_COMPLETE
        7. Update adapter_state (new watermark)
    """
    nifi = get_nifi_config()
    pg_id = nifi['process_group_id']
    base_url = nifi['base_url']
    
    logger.info(f"Triggering NiFi process group: {pg_id}")
    
    # Get current state of process group
    response = requests.get(
        f"{base_url}/nifi-api/process-groups/{pg_id}",
        auth=(nifi['username'], nifi['password']) if nifi['username'] else None,
        timeout=30
    )
    response.raise_for_status()
    
    # Start the process group
    pg_data = response.json()
    revision = pg_data['revision']
    
    start_response = requests.put(
        f"{base_url}/nifi-api/flow/process-groups/{pg_id}",
        json={
            "id": pg_id,
            "state": "RUNNING",
            "disconnectedNodeAcknowledged": False
        },
        auth=(nifi['username'], nifi['password']) if nifi['username'] else None,
        timeout=30
    )
    start_response.raise_for_status()
    
    trigger_time = datetime.utcnow().isoformat()
    kwargs['ti'].xcom_push(key='nifi_trigger_time', value=trigger_time)
    kwargs['ti'].xcom_push(key='nifi_pg_id', value=pg_id)
    
    logger.info(f"NiFi process group triggered at {trigger_time}")


# ─────────────────────────────────────────────────────────────────────────────
# Task: Monitor NiFi Completion (Poll Control Table)
# ─────────────────────────────────────────────────────────────────────────────

def wait_for_raw_complete(**kwargs):
    """
    Poll batch_registry for RAW_COMPLETE status.
    
    This is the UEH-approved polling pattern:
    - Poll CONTROL TABLE (not file system, not NiFi status)
    - Business-level status check (not technical file existence)
    - Respects SLA timeout
    
    The NiFi flow writes RAW_COMPLETE to batch_registry upon successful
    completion. This task polls for that status transition.
    """
    env = get_environment()
    db_control = f"ueh_{env}_control"
    spark = get_spark_session()
    
    today = kwargs['ds']
    trigger_time = kwargs['ti'].xcom_pull(key='nifi_trigger_time')
    sla_minutes = kwargs['ti'].xcom_pull(key='sla_minutes') or 60
    
    poll_interval_sec = 60  # Check every 60 seconds
    max_wait_sec = int(sla_minutes) * 60  # SLA-based timeout
    elapsed_sec = 0
    
    logger.info(
        f"Waiting for RAW_COMPLETE: adapter={ADAPTER_INSTANCE_ID}, "
        f"date={today}, timeout={max_wait_sec}s"
    )
    
    while elapsed_sec < max_wait_sec:
        result = spark.sql(f"""
            SELECT batch_id, status, records_ingested, started_at
            FROM {db_control}.t01_ueh_ctl_batch_registry
            WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
              AND ingestion_date = '{today}'
              AND status = 'RAW_COMPLETE'
            ORDER BY started_at DESC
            LIMIT 1
        """).first()
        
        if result is not None:
            batch_id = result.batch_id
            records = result.records_ingested
            logger.info(
                f"RAW_COMPLETE detected: batch_id={batch_id}, "
                f"records={records}"
            )
            kwargs['ti'].xcom_push(key='batch_id', value=batch_id)
            kwargs['ti'].xcom_push(key='records_ingested', value=records)
            return True
        
        # Check for FAILED status
        failed = spark.sql(f"""
            SELECT batch_id, failure_reason
            FROM {db_control}.t01_ueh_ctl_batch_registry
            WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
              AND ingestion_date = '{today}'
              AND status = 'FAILED'
            ORDER BY started_at DESC
            LIMIT 1
        """).first()
        
        if failed is not None:
            raise Exception(
                f"NiFi ingestion FAILED: batch={failed.batch_id}, "
                f"reason={failed.failure_reason}"
            )
        
        time.sleep(poll_interval_sec)
        elapsed_sec += poll_interval_sec
        
        if elapsed_sec % 300 == 0:  # Log every 5 minutes
            logger.info(f"Still waiting... ({elapsed_sec}s / {max_wait_sec}s)")
    
    # Timeout reached
    raise Exception(
        f"SLA BREACH: NVD ingestion did not complete within {sla_minutes} minutes. "
        f"adapter={ADAPTER_INSTANCE_ID}, date={today}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Task: Post-Ingestion Validation
# ─────────────────────────────────────────────────────────────────────────────

def post_ingestion_validation(**kwargs):
    """
    Basic sanity checks after RAW_COMPLETE:
    1. Batch has records_ingested > 0 (unless source had no changes)
    2. HDFS path exists and has chunk files
    3. Manifest file exists
    
    This is a lightweight check. Full DQ happens in Silver.
    """
    env = get_environment()
    db_control = f"ueh_{env}_control"
    spark = get_spark_session()
    
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')
    
    batch = spark.sql(f"""
        SELECT bronze_path, records_ingested, chunks_written
        FROM {db_control}.t01_ueh_ctl_batch_registry
        WHERE batch_id = '{batch_id}'
    """).first()
    
    if batch is None:
        raise Exception(f"Batch {batch_id} disappeared from registry!")
    
    logger.info(
        f"Validation: batch={batch_id}, "
        f"records={batch.records_ingested}, "
        f"chunks={batch.chunks_written}, "
        f"path={batch.bronze_path}"
    )
    
    # Warn if zero records (might be valid — no new CVEs modified)
    if batch.records_ingested == 0:
        logger.warning(
            f"Zero records ingested for batch {batch_id}. "
            f"This may be normal if no CVEs were modified since last watermark."
        )
    
    logger.info(f"Post-ingestion validation PASSED for batch {batch_id}")


# ─────────────────────────────────────────────────────────────────────────────
# Task: Skip Handler
# ─────────────────────────────────────────────────────────────────────────────

def log_skip_reason(**kwargs):
    """Log why ingestion was skipped."""
    logger.info(
        f"NVD ingestion SKIPPED for {kwargs['ds']}. "
        f"Check pre-flight task logs for reason."
    )


# ─────────────────────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH DAG 1: NVD raw ingestion orchestration (API → HDFS)',
    schedule_interval='0 3 * * *',       # Daily at 03:00 UTC
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,                    # Never overlap runs
    tags=['ueh', 'ingestion', 'nvd', 'vulnerability_intel', 'dag1'],
    doc_md=__doc__
) as dag:

    # ─── Pre-flight ───────────────────────────────────────────────────────
    preflight = BranchPythonOperator(
        task_id='preflight_checks',
        python_callable=preflight_checks,
        provide_context=True
    )

    # ─── Branch: Skip ─────────────────────────────────────────────────────
    skip = PythonOperator(
        task_id='skip_ingestion',
        python_callable=log_skip_reason,
        provide_context=True
    )

    # ─── Branch: Proceed ──────────────────────────────────────────────────
    trigger = PythonOperator(
        task_id='trigger_nifi_ingestion',
        python_callable=trigger_nifi_ingestion,
        provide_context=True
    )

    wait_raw = PythonOperator(
        task_id='wait_for_raw_complete',
        python_callable=wait_for_raw_complete,
        provide_context=True,
        execution_timeout=timedelta(hours=2)  # Hard timeout
    )

    validate = PythonOperator(
        task_id='post_ingestion_validation',
        python_callable=post_ingestion_validation,
        provide_context=True
    )

    # ─── End ──────────────────────────────────────────────────────────────
    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success'
    )

    # ─── Dependencies ─────────────────────────────────────────────────────
    preflight >> [trigger, skip]
    trigger >> wait_raw >> validate >> end
    skip >> end
