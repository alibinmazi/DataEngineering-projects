"""
=============================================================================
UEH Generic Bronze Load DAG (DAG 2)
=============================================================================
ONE DAG that loads Bronze for ALL adapters.

How it works:
    1. Query batch_registry for ANY RAW_COMPLETE batches (across all adapters)
    2. For each pending batch: submit generic_bronze_loader.py Spark job
    3. Verify BRONZE_COMPLETE after each

Schedule: Every 10 minutes (responsive polling)
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

DAG_ID = "ueh_generic_bronze_load"
DB_CONTROL = "t01_ueh_dev_ctl"
SPARK_JOB = "/apps/ueh/spark/generic_bronze_loader.py"

default_args = {
    'owner': 'ueh-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("UEH_DAG2_Generic") \
        .enableHiveSupport() \
        .getOrCreate()


def find_pending_batch(**kwargs):
    """Find oldest RAW_COMPLETE batch across ALL adapters."""
    spark = get_spark()

    pending = spark.sql(f"""
        SELECT batch_id, adapter_instance_id, bronze_path, records_expected
        FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
        WHERE batch_status = 'RAW_COMPLETE'
        ORDER BY created_at ASC
        LIMIT 1
    """).first()

    if pending is None:
        logger.info("No RAW_COMPLETE batches pending.")
        return False

    logger.info(f"Found: {pending.batch_id} ({pending.adapter_instance_id})")
    kwargs['ti'].xcom_push(key='batch_id', value=pending.batch_id)
    return True


def verify_complete(**kwargs):
    """Verify the batch is now BRONZE_COMPLETE."""
    spark = get_spark()
    batch_id = kwargs['ti'].xcom_pull(key='batch_id')

    result = spark.sql(f"""
        SELECT batch_status, records_processed
        FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
        WHERE batch_id = '{batch_id}'
    """).first()

    if result.batch_status == 'BRONZE_COMPLETE':
        logger.info(f"VERIFIED: {batch_id} → BRONZE_COMPLETE ({result.records_processed} records)")
    elif result.batch_status == 'FAILED':
        raise Exception(f"Batch {batch_id} FAILED during Bronze load")
    else:
        raise Exception(f"Unexpected status: {result.batch_status}")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH DAG 2: Load RAW_COMPLETE batches into Bronze (all adapters)',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'platform', 'bronze', 'generic'],
) as dag:

    t1 = ShortCircuitOperator(
        task_id='find_pending_batch',
        python_callable=find_pending_batch,
    )

    t2 = SparkSubmitOperator(
        task_id='run_generic_bronze_loader',
        application=SPARK_JOB,
        name='UEH_Bronze_{{ ti.xcom_pull(key="batch_id") }}',
        application_args=['{{ ti.xcom_pull(key="batch_id") }}'],
        conf={
            'ueh.environment': Variable.get("ueh_environment", default_var="dev"),
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
            'spark.sql.catalog.spark_catalog.type': 'hive',
        },
    )

    t3 = PythonOperator(
        task_id='verify_bronze_complete',
        python_callable=verify_complete,
    )

    t1 >> t2 >> t3
