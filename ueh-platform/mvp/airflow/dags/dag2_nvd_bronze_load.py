"""
=============================================================================
UEH MVP — DAG 2: NVD Bronze Load (HDFS → Iceberg)
=============================================================================
What it does:
    1. Poll batch_registry for RAW_COMPLETE batches
    2. If found, submit Spark job to load into Bronze Iceberg table
    3. Verify BRONZE_COMPLETE status after Spark finishes

Schedule: Every 30 minutes (responsive pickup)
Coupling: Reads batch_registry.status = 'RAW_COMPLETE' (written by NiFi via DAG 1)

This DAG is DECOUPLED from DAG 1:
    - DAG 1 failure doesn't affect DAG 2
    - DAG 2 can retry Bronze loading from existing HDFS data
    - DAG 2 can process replay batches without DAG 1
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
DAG_ID = "ueh_dag2_nvd_bronze_load"
ADAPTER_INSTANCE_ID = "nvd_public_01"
SPARK_JOB_PATH = "/apps/ueh/spark/bronze_nvd_loader.py"

default_args = {
    'owner': 'ueh',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}


# ─── Helper ──────────────────────────────────────────────────────────────────
def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName(f"UEH_DAG2_{ADAPTER_INSTANCE_ID}") \
        .enableHiveSupport() \
        .getOrCreate()


# ─── Task 1: Find Pending Batch ──────────────────────────────────────────────
def find_pending_batch(**kwargs):
    """
    Check batch_registry for any RAW_COMPLETE batch that needs Bronze loading.
    Returns True if found (proceed), False if nothing to do (skip).
    
    Picks the OLDEST pending batch (FIFO order).
    """
    spark = get_spark()

    pending = spark.sql(f"""
        SELECT batch_id, ingestion_date, records_ingested, bronze_path
        FROM ueh_dev_control.t01_ueh_ctl_batch_registry
        WHERE adapter_instance_id = '{ADAPTER_INSTANCE_ID}'
          AND status = 'RAW_COMPLETE'
        ORDER BY created_at ASC
        LIMIT 1
    """).first()

    if pending is None:
        logger.info("No RAW_COMPLETE batches pending. Nothing to do.")
        return False

    logger.info(
        f"Found pending batch: {pending.batch_id} "
        f"(date={pending.ingestion_date}, records={pending.records_ingested})"
    )
    kwargs['ti'].xcom_push(key='batch_id', value=pending.batch_id)
    kwargs['ti'].xcom_push(key='bronze_path', value=pending.bronze_path)
    return True


# ─── Task 2: Run Spark Job ───────────────────────────────────────────────────
# NOTE: If using CDE (Cloudera Data Engineering), replace SparkSubmitOperator
# with a PythonOperator that calls CDE REST API:
#   cde spark submit --application-file bronze_nvd_loader.py -- <batch_id>


# ─── Task 3: Verify Bronze Load ──────────────────────────────────────────────
def verify_bronze_complete(**kwargs):
    """
    Confirm the Spark job succeeded:
    1. batch_registry.status = BRONZE_COMPLETE
    2. Records exist in Bronze Iceberg table
    """
    spark = get_spark()
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')

    # Check registry status
    batch = spark.sql(f"""
        SELECT status, records_ingested
        FROM ueh_dev_control.t01_ueh_ctl_batch_registry
        WHERE batch_id = '{batch_id}'
    """).first()

    if batch.status != 'BRONZE_COMPLETE':
        raise Exception(f"Expected BRONZE_COMPLETE but got '{batch.status}'")

    # Quick count check
    actual = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM ueh_dev_bronze.t01_ueh_brz_nvd_vulnerabilities
        WHERE batch_id = '{batch_id}'
    """).first().cnt

    logger.info(f"VERIFIED: batch={batch_id}, status=BRONZE_COMPLETE, records={actual}")

    if actual == 0:
        raise Exception(f"Zero records in Bronze table for batch {batch_id}!")


# ─── DAG Definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH DAG 2: Load RAW_COMPLETE batches into Bronze Iceberg',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'nvd', 'dag2', 'bronze', 'spark'],
) as dag:

    t1_find = ShortCircuitOperator(
        task_id='find_pending_batch',
        python_callable=find_pending_batch,
    )

    t2_spark = SparkSubmitOperator(
        task_id='run_bronze_loader',
        application=SPARK_JOB_PATH,
        name='UEH_Bronze_NVD_{{ ti.xcom_pull(key="batch_id") }}',
        application_args=['{{ ti.xcom_pull(key="batch_id") }}'],
        conf={
            'ueh.environment': Variable.get("ueh_environment", default_var="dev"),
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
            'spark.sql.catalog.spark_catalog.type': 'hive',
            'spark.driver.memory': '4g',
            'spark.executor.memory': '8g',
            'spark.executor.instances': '2',
        },
        verbose=True,
    )

    t3_verify = PythonOperator(
        task_id='verify_bronze_complete',
        python_callable=verify_bronze_complete,
    )

    t1_find >> t2_spark >> t3_verify
