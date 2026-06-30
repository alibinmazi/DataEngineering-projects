"""
=============================================================================
UEH Platform: DAG 2 - NVD Bronze Load (HDFS Raw → Iceberg Bronze Table)
=============================================================================
DAG Name:    ueh_bronze_load_nvd
Purpose:     Detect RAW_COMPLETE batches and load them into Bronze Iceberg table.
             Transitions batch from RAW_COMPLETE → BRONZE_COMPLETE.

Responsibility:
    - Poll batch_registry for RAW_COMPLETE batches (NVD adapter)
    - Submit Spark job to load raw chunks into Bronze Iceberg table
    - Validate Bronze load success
    - Update batch_registry → BRONZE_COMPLETE

Does NOT:
    - Trigger any ingestion (that's DAG 1)
    - Connect to NVD API
    - Touch Silver/Gold layers

Trigger:     Scheduled every 30 minutes (frequent polling for responsiveness)
             OR event-driven via Airflow dataset/trigger rules (future)
Depends On:  batch_registry.status = 'RAW_COMPLETE' (polled, not sensored)
Triggers:    Nothing (Silver DAG will poll for BRONZE_COMPLETE independently)

Why Separate from DAG 1:
    1. Failure isolation — NiFi failure doesn't prevent Bronze retry
    2. Replay support — can re-run Bronze from existing HDFS data
    3. Independent scheduling — ingestion daily, Bronze load can retry immediately
    4. Team ownership — ingestion team vs data engineering team
    5. Clean status lifecycle — RAW_COMPLETE → BRONZE_COMPLETE is atomic
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

DAG_ID = "ueh_bronze_load_nvd"
ADAPTER_INSTANCE_ID = "nvd_public_01"
ADAPTER_NAME = "nvd"

# Spark job location (on HDFS or local path accessible to CDE/Spark cluster)
SPARK_JOB_PATH = "/apps/ueh/spark/bronze/bronze_nvd_loader.py"
SPARK_BASE_MODULE_PATH = "/apps/ueh/spark/bronze/base_bronze_loader.py"

default_args = {
    'owner': 'ueh-data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['ueh-alerts@company.com'],
    'retries': 3,                          # More retries — Bronze load is idempotent
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=45),
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


# ─────────────────────────────────────────────────────────────────────────────
# Task: Check for Pending Batches
# ─────────────────────────────────────────────────────────────────────────────

def check_pending_batches(**kwargs):
    """
    Poll batch_registry for RAW_COMPLETE batches that need Bronze loading.
    
    This is the UEH DECOUPLING mechanism:
    - DAG 1 writes RAW_COMPLETE
    - DAG 2 polls for RAW_COMPLETE and processes
    - No direct DAG-to-DAG dependency
    - No file sensors
    - Pure control-table-driven orchestration
    
    Returns: True if pending batch found, False to short-circuit DAG
    """
    env = get_environment()
    db_control = f"ueh_{env}_control"
    spark = get_spark_session()
    
    # Find oldest RAW_COMPLETE batch for this adapter (FIFO processing)
    pending = spark.sql(f"""
        SELECT 
            batch_id,
            ingestion_date,
            load_type,
            records_ingested,
            bronze_path,
            started_at
        FROM {db_control}.t01_ueh_ctl_batch_registry
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
          AND status = 'RAW_COMPLETE'
        ORDER BY started_at ASC
        LIMIT 1
    """).first()
    
    if pending is None:
        logger.info(
            f"No pending RAW_COMPLETE batches for '{ADAPTER_INSTANCE_ID}'. "
            f"Nothing to process."
        )
        return False  # ShortCircuit → skip remaining tasks
    
    batch_id = pending.batch_id
    logger.info(
        f"Found pending batch: {batch_id} "
        f"(date={pending.ingestion_date}, records={pending.records_ingested}, "
        f"path={pending.bronze_path})"
    )
    
    # Push batch details for downstream tasks
    kwargs['ti'].xcom_push(key='batch_id', value=batch_id)
    kwargs['ti'].xcom_push(key='ingestion_date', value=str(pending.ingestion_date))
    kwargs['ti'].xcom_push(key='bronze_path', value=pending.bronze_path)
    kwargs['ti'].xcom_push(key='expected_records', value=pending.records_ingested)
    
    return True  # Proceed with Bronze load


# ─────────────────────────────────────────────────────────────────────────────
# Task: Submit Bronze Spark Job
# ─────────────────────────────────────────────────────────────────────────────

def build_spark_submit_args(**kwargs):
    """
    Prepare arguments for SparkSubmitOperator.
    This runs as a pre-task to resolve XCom values into Spark args.
    """
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')
    
    if not batch_id:
        raise Exception("No batch_id found in XCom. check_pending_batches may have failed.")
    
    logger.info(f"Preparing Spark submit for batch: {batch_id}")
    kwargs['ti'].xcom_push(key='spark_batch_id', value=batch_id)


# ─────────────────────────────────────────────────────────────────────────────
# Task: Validate Bronze Load
# ─────────────────────────────────────────────────────────────────────────────

def validate_bronze_load(**kwargs):
    """
    Post-load validation:
    1. Verify batch_registry status = BRONZE_COMPLETE
    2. Verify records exist in Bronze Iceberg table for this batch
    3. Compare record count with expected (from RAW_COMPLETE)
    4. Basic DQ: check for NULL payload_json
    """
    env = get_environment()
    db_control = f"ueh_{env}_control"
    db_bronze = f"ueh_{env}_bronze"
    spark = get_spark_session()
    
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')
    expected_records = kwargs['ti'].xcom_pull(key='expected_records')
    
    # Check 1: Status should be BRONZE_COMPLETE
    batch = spark.sql(f"""
        SELECT status, records_ingested, bronze_completed_at
        FROM {db_control}.t01_ueh_ctl_batch_registry
        WHERE batch_id = '{batch_id}'
    """).first()
    
    if batch is None:
        raise Exception(f"Batch {batch_id} not found in registry!")
    
    if batch.status != 'BRONZE_COMPLETE':
        raise Exception(
            f"Expected BRONZE_COMPLETE but got '{batch.status}' for batch {batch_id}. "
            f"Spark job may have failed silently."
        )
    
    # Check 2: Records exist in Bronze table
    actual_count = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM {db_bronze}.t01_ueh_brz_nvd_vulnerabilities
        WHERE batch_id = '{batch_id}'
    """).first().cnt
    
    if actual_count == 0:
        raise Exception(
            f"Zero records found in Bronze table for batch {batch_id}!"
        )
    
    # Check 3: Record count sanity
    # Allow some variance (NVD may have nested structures)
    if expected_records and expected_records > 0:
        variance = abs(actual_count - expected_records) / max(expected_records, 1)
        if variance > 0.1:  # More than 10% difference
            logger.warning(
                f"Record count mismatch: expected={expected_records}, "
                f"actual={actual_count}, variance={variance:.1%}"
            )
    
    # Check 4: No NULL payloads
    null_payloads = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM {db_bronze}.t01_ueh_brz_nvd_vulnerabilities
        WHERE batch_id = '{batch_id}'
          AND (payload_json IS NULL OR payload_json = '')
    """).first().cnt
    
    if null_payloads > 0:
        logger.warning(
            f"Found {null_payloads} records with NULL/empty payload_json "
            f"in batch {batch_id}"
        )
    
    logger.info(
        f"Bronze load VALIDATED: batch={batch_id}, "
        f"records={actual_count}, null_payloads={null_payloads}, "
        f"completed_at={batch.bronze_completed_at}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Task: Cleanup / Notification
# ─────────────────────────────────────────────────────────────────────────────

def notify_completion(**kwargs):
    """
    Log successful completion. Future: send notification or trigger downstream.
    """
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')
    logger.info(
        f"NVD Bronze load pipeline COMPLETE for batch {batch_id}. "
        f"Downstream Silver DAG can now process this batch."
    )


# ─────────────────────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH DAG 2: Load RAW_COMPLETE batches into Bronze Iceberg (NVD)',
    schedule_interval='*/30 * * * *',    # Every 30 minutes (responsive polling)
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,                    # Process one batch at a time
    tags=['ueh', 'bronze', 'nvd', 'vulnerability_intel', 'dag2', 'spark'],
    doc_md=__doc__
) as dag:

    # ─── Check for work ───────────────────────────────────────────────────
    check_pending = ShortCircuitOperator(
        task_id='check_pending_batches',
        python_callable=check_pending_batches,
        provide_context=True
    )

    # ─── Prepare Spark args ───────────────────────────────────────────────
    prep_spark = PythonOperator(
        task_id='prepare_spark_args',
        python_callable=build_spark_submit_args,
        provide_context=True
    )

    # ─── Submit Spark Bronze Load Job ─────────────────────────────────────
    # NOTE: For CDE (Cloudera Data Engineering), replace SparkSubmitOperator
    # with CDEJobRunOperator or a PythonOperator calling CDE API
    bronze_load = SparkSubmitOperator(
        task_id='spark_bronze_load',
        application=SPARK_JOB_PATH,
        name=f'UEH_Bronze_NVD_{{{{ ti.xcom_pull(key="batch_id") }}}}',
        application_args=[
            '{{ ti.xcom_pull(key="batch_id") }}'
        ],
        py_files=SPARK_BASE_MODULE_PATH,
        conf={
            'ueh.environment': '{{ var.value.ueh_environment }}',
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
            'spark.sql.catalog.spark_catalog.type': 'hive',
            'spark.driver.memory': '4g',
            'spark.executor.memory': '8g',
            'spark.executor.cores': '4',
            'spark.executor.instances': '2',
            'spark.dynamicAllocation.enabled': 'true',
            'spark.dynamicAllocation.minExecutors': '1',
            'spark.dynamicAllocation.maxExecutors': '5',
        },
        verbose=True
    )

    # ─── Validate ─────────────────────────────────────────────────────────
    validate = PythonOperator(
        task_id='validate_bronze_load',
        python_callable=validate_bronze_load,
        provide_context=True
    )

    # ─── Notify ───────────────────────────────────────────────────────────
    notify = PythonOperator(
        task_id='notify_completion',
        python_callable=notify_completion,
        provide_context=True
    )

    # ─── Dependencies ─────────────────────────────────────────────────────
    check_pending >> prep_spark >> bronze_load >> validate >> notify
