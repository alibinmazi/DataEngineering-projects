"""
=============================================================================
UEH Generic Silver Transform DAG (DAG 3)
=============================================================================
ONE DAG that transforms Bronze → Silver for ALL adapters.

How it works:
    1. Poll batch_registry for ANY BRONZE_COMPLETE batches (across all adapters)
    2. For each pending batch: submit generic_silver_transformer.py Spark job
    3. Verify SILVER_COMPLETE after Spark finishes

Schedule: Every 15 minutes (responsive polling)
Coupling: batch_registry.batch_status = 'BRONZE_COMPLETE' → 'SILVER_COMPLETE'

Pipeline position:
    DAG 1 (Ingestion) → RAW_COMPLETE
    DAG 2 (Bronze)    → BRONZE_COMPLETE
    DAG 3 (Silver)    → SILVER_COMPLETE  ← THIS DAG
    DAG 4 (Gold)      → GOLD_COMPLETE    (future)

Database: t01_ueh_dev_ctl (control), t01_ueh_dev_slv (silver)
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
DAG_ID = "ueh_generic_silver_transform"
DB_CONTROL = "t01_ueh_dev_ctl"
DB_SILVER = "t01_ueh_dev_slv"
SPARK_JOB = "/apps/ueh/spark/generic_silver_transformer.py"

default_args = {
    'owner': 'ueh-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=60),
}


# ─── Helper ──────────────────────────────────────────────────────────────────
def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("UEH_DAG3_Silver") \
        .enableHiveSupport() \
        .getOrCreate()


# ─── Task 1: Find Pending Batch ──────────────────────────────────────────────
def find_pending_batch(**kwargs):
    """
    Find oldest BRONZE_COMPLETE batch across ALL adapters.
    
    This is the decoupling pattern:
    - DAG 2 writes BRONZE_COMPLETE
    - DAG 3 polls for BRONZE_COMPLETE
    - No DAG-to-DAG dependency, only control table coupling
    
    Returns True if found (proceed), False if nothing to do (skip).
    """
    spark = get_spark()

    pending = spark.sql(f"""
        SELECT 
            b.batch_id, 
            b.adapter_instance_id, 
            b.ingestion_date,
            b.records_processed,
            a.source_system
        FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry b
        JOIN {DB_CONTROL}.t01_ueh_ctl_adapter_config a
          ON b.adapter_instance_id = a.adapter_instance_id
        WHERE b.batch_status = 'BRONZE_COMPLETE'
        ORDER BY b.created_at ASC
        LIMIT 1
    """).first()

    if pending is None:
        logger.info("No BRONZE_COMPLETE batches pending for Silver. Nothing to do.")
        return False

    logger.info(
        f"Found pending batch: {pending.batch_id} "
        f"(source={pending.source_system}, "
        f"adapter={pending.adapter_instance_id}, "
        f"date={pending.ingestion_date}, "
        f"bronze_records={pending.records_processed})"
    )

    kwargs['ti'].xcom_push(key='batch_id', value=pending.batch_id)
    kwargs['ti'].xcom_push(key='source_system', value=pending.source_system)
    kwargs['ti'].xcom_push(key='adapter_instance_id', value=pending.adapter_instance_id)
    kwargs['ti'].xcom_push(key='records_processed', value=pending.records_processed)
    return True


# ─── Task 3: Verify Silver Complete ──────────────────────────────────────────
def verify_silver_complete(**kwargs):
    """
    Confirm the Spark job succeeded:
    1. batch_registry.batch_status = 'SILVER_COMPLETE'
    2. Log record count and any warnings
    
    If FAILED, raise exception for Airflow retry/alerting.
    """
    spark = get_spark()
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')
    source_system = kwargs['ti'].xcom_pull(key='source_system')
    bronze_records = kwargs['ti'].xcom_pull(key='records_processed') or 0

    result = spark.sql(f"""
        SELECT batch_status, records_processed, failure_reason
        FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
        WHERE batch_id = '{batch_id}'
    """).first()

    if result is None:
        raise Exception(f"Batch {batch_id} disappeared from registry!")

    if result.batch_status == 'FAILED':
        raise Exception(
            f"Silver transformation FAILED for batch {batch_id} "
            f"(source={source_system}): {result.failure_reason}"
        )

    if result.batch_status != 'SILVER_COMPLETE':
        raise Exception(
            f"Expected SILVER_COMPLETE but got '{result.batch_status}' "
            f"for batch {batch_id}"
        )

    silver_records = result.records_processed or 0

    logger.info(
        f"VERIFIED: batch={batch_id}, source={source_system}, "
        f"status=SILVER_COMPLETE, silver_records={silver_records}"
    )

    # Warn if significant record count difference (Bronze vs Silver)
    if bronze_records and bronze_records > 0 and silver_records > 0:
        ratio = silver_records / bronze_records
        if ratio < 0.5:
            logger.warning(
                f"⚠️ Silver produced significantly fewer records than Bronze: "
                f"bronze={bronze_records}, silver={silver_records}, ratio={ratio:.2f}. "
                f"Check if field_mapping is filtering/failing records."
            )
        elif ratio > 1.5:
            logger.warning(
                f"⚠️ Silver produced more records than Bronze: "
                f"bronze={bronze_records}, silver={silver_records}. "
                f"Check if explosion/duplication is occurring."
            )


# ─── Task 4: Log Completion ──────────────────────────────────────────────────
def log_completion(**kwargs):
    """Log successful Silver completion for operational tracking."""
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')
    source_system = kwargs['ti'].xcom_pull(key='source_system')
    logger.info(
        f"DAG 3 COMPLETE: {batch_id} ({source_system}) → SILVER_COMPLETE. "
        f"Data is now available in Silver tables for analytics/Gold layer."
    )


# ─── DAG Definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH DAG 3: Transform BRONZE_COMPLETE batches into Silver (all adapters)',
    schedule_interval='*/15 * * * *',   # Every 15 minutes
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'platform', 'silver', 'generic', 'transform'],
    doc_md=__doc__
) as dag:

    # ─── Find pending BRONZE_COMPLETE batch ───────────────────────────
    t1_find = ShortCircuitOperator(
        task_id='find_pending_batch',
        python_callable=find_pending_batch,
    )

    # ─── Submit Silver Spark job ──────────────────────────────────────
    # NOTE: For CDE, replace SparkSubmitOperator with PythonOperator
    # calling CDE REST API:
    #   cde spark submit --application-file generic_silver_transformer.py \
    #       -- --batch_id <batch_id>
    t2_spark = SparkSubmitOperator(
        task_id='run_silver_transformer',
        application=SPARK_JOB,
        name='UEH_Silver_{{ ti.xcom_pull(key="source_system") }}_{{ ti.xcom_pull(key="batch_id") }}',
        application_args=[
            '--batch_id', '{{ ti.xcom_pull(key="batch_id") }}'
        ],
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
        task_id='verify_silver_complete',
        python_callable=verify_silver_complete,
    )

    # ─── Log success ──────────────────────────────────────────────────
    t4_done = PythonOperator(
        task_id='log_completion',
        python_callable=log_completion,
    )

    # ─── Dependencies ─────────────────────────────────────────────────
    t1_find >> t2_spark >> t3_verify >> t4_done
