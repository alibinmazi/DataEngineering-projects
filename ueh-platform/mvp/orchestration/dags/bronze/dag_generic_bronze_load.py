"""
=============================================================================
UEH Generic Bronze Load DAG (DAG 2)
=============================================================================
ONE DAG that handles Bronze loading for ALL adapters.

How it works:
    1. Polls batch_registry for ANY RAW_COMPLETE batch (regardless of adapter)
    2. Reads source_system from adapter_config to determine Bronze table
    3. Submits the generic_bronze_loader.py Spark job
    4. Verifies BRONZE_COMPLETE after Spark finishes

Schedule: Every 10 minutes (responsive pickup)
Coupling: batch_registry.batch_status = 'RAW_COMPLETE' → 'BRONZE_COMPLETE'

This DAG is ADAPTER-AGNOSTIC:
    - Does NOT know which adapters exist
    - Does NOT care about schedules
    - Just finds RAW_COMPLETE batches and loads them
    - Works for NVD, Tenable, ADDM, or any future adapter

Database: t01_ueh_dev_ctl
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
DAG_ID = "ueh_generic_bronze_load"
DB_CONTROL = "t01_ueh_dev_ctl"
SPARK_JOB = "/apps/ueh/spark/generic_bronze_loader.py"

default_args = {
    'owner': 'ueh-platform',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=45),
}


# ─── Helper ──────────────────────────────────────────────────────────────────
def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("UEH_DAG2_GenericBronze") \
        .enableHiveSupport() \
        .getOrCreate()


# ─── Task 1: Find Pending Batch ──────────────────────────────────────────────
def find_pending_batch(**kwargs):
    """
    Find the OLDEST RAW_COMPLETE batch across ALL adapters.
    
    This is the key decoupling mechanism:
    - DAG 1 (per-adapter) writes RAW_COMPLETE
    - This DAG polls for ANY RAW_COMPLETE
    - No adapter-specific logic needed here
    
    Returns True if found (proceed), False if nothing to do (skip).
    """
    spark = get_spark()

    try:
        pending = spark.sql(f"""
            SELECT 
                b.batch_id,
                b.adapter_instance_id,
                b.org_id,
                b.ingestion_date,
                b.records_expected,
                b.bronze_path,
                a.source_system
            FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry b
            JOIN {DB_CONTROL}.t01_ueh_ctl_adapter_config a
              ON b.adapter_instance_id = a.adapter_instance_id
              AND b.org_id = a.org_id
            WHERE b.batch_status = 'RAW_COMPLETE'
            ORDER BY b.created_at ASC
            LIMIT 1
        """).first()

        if pending is None:
            logger.info("No RAW_COMPLETE batches pending. Nothing to do.")
            return False

        logger.info(
            f"Found pending batch: {pending.batch_id} "
            f"(source={pending.source_system}, "
            f"adapter={pending.adapter_instance_id}, "
            f"date={pending.ingestion_date}, "
            f"records={pending.records_expected}, "
            f"path={pending.bronze_path})"
        )

        # Push to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='batch_id', value=pending.batch_id)
        kwargs['ti'].xcom_push(key='source_system', value=pending.source_system)
        kwargs['ti'].xcom_push(key='adapter_instance_id', value=pending.adapter_instance_id)
        kwargs['ti'].xcom_push(key='records_expected', value=pending.records_expected)
        kwargs['ti'].xcom_push(key='bronze_path', value=pending.bronze_path)
        return True

    finally:
        spark.stop()


# ─── Task 3: Verify Bronze Complete ──────────────────────────────────────────
def verify_bronze_complete(**kwargs):
    """
    Confirm the Spark job succeeded:
    1. batch_registry.batch_status = 'BRONZE_COMPLETE'
    2. Records exist in correct Bronze Iceberg table
    3. Record count is reasonable vs expected
    """
    spark = get_spark()

    try:
        batch_id = kwargs['ti'].xcom_pull(key='batch_id')
        source_system = kwargs['ti'].xcom_pull(key='source_system')
        records_expected = kwargs['ti'].xcom_pull(key='records_expected') or 0

        # Check registry status
        result = spark.sql(f"""
            SELECT batch_status, records_processed, failure_reason
            FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
            WHERE batch_id = '{batch_id}'
        """).first()

        if result is None:
            raise Exception(f"Batch {batch_id} disappeared from registry!")

        if result.batch_status == 'FAILED':
            raise Exception(
                f"Bronze load FAILED for batch {batch_id} "
                f"(source={source_system}): {result.failure_reason}"
            )

        if result.batch_status != 'BRONZE_COMPLETE':
            raise Exception(
                f"Expected BRONZE_COMPLETE but got '{result.batch_status}' "
                f"for batch {batch_id}"
            )

        # Quick count check in Bronze table
        bronze_table = f"t01_ueh_dev_brz.t01_ueh_brz_{source_system.lower()}_raw"
        actual = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {bronze_table}
            WHERE batch_id = '{batch_id}'
        """).first().cnt

        if actual == 0:
            raise Exception(
                f"Zero records in Bronze table {bronze_table} "
                f"for batch {batch_id}!"
            )

        logger.info(
            f"VERIFIED: batch={batch_id}, source={source_system}, "
            f"status=BRONZE_COMPLETE, "
            f"records_in_bronze={actual}, expected={records_expected}"
        )

        # Warn on significant variance
        if records_expected and records_expected > 0:
            variance = abs(actual - records_expected) / max(records_expected, 1)
            if variance > 0.2:
                logger.warning(
                    f"Record count variance: expected={records_expected}, "
                    f"actual={actual}, variance={variance:.1%}"
                )

    finally:
        spark.stop()


# ─── Task 4: Log Completion ──────────────────────────────────────────────────
def log_completion(**kwargs):
    """Log successful Bronze load."""
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')
    source_system = kwargs['ti'].xcom_pull(key='source_system')
    logger.info(
        f"Bronze load COMPLETE: {batch_id} ({source_system}) → BRONZE_COMPLETE. "
        f"Silver DAG will pick it up."
    )


# ─── DAG Definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH DAG 2: Generic Bronze load (any adapter, polls RAW_COMPLETE)',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'bronze', 'generic', 'all-adapters'],
    doc_md=__doc__
) as dag:

    # ─── Find pending RAW_COMPLETE batch ──────────────────────────────
    t1_find = ShortCircuitOperator(
        task_id='find_pending_batch',
        python_callable=find_pending_batch,
    )

    # ─── Submit Generic Bronze Loader Spark Job ───────────────────────
    # NOTE: For CDE, replace with PythonOperator calling CDE REST API
    t2_spark = SparkSubmitOperator(
        task_id='run_generic_bronze_loader',
        application=SPARK_JOB,
        name='UEH_Bronze_{{ ti.xcom_pull(key="source_system") }}_{{ ti.xcom_pull(key="batch_id") }}',
        application_args=['{{ ti.xcom_pull(key="batch_id") }}'],
        conf={
            'ueh.environment': Variable.get("ueh_environment", default_var="dev"),
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
        verbose=True,
    )

    # ─── Verify completion ────────────────────────────────────────────
    t3_verify = PythonOperator(
        task_id='verify_bronze_complete',
        python_callable=verify_bronze_complete,
    )

    # ─── Log success ──────────────────────────────────────────────────
    t4_done = PythonOperator(
        task_id='log_completion',
        python_callable=log_completion,
    )

    # ─── Dependencies ─────────────────────────────────────────────────
    t1_find >> t2_spark >> t3_verify >> t4_done
