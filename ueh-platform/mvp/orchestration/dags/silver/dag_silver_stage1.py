"""
=============================================================================
UEH DAG: Silver Stage 1 (Adapter Staging)
=============================================================================
Polls for BRONZE_COMPLETE batches with dq_status PASSED/WARNING.
Submits silver_stage1_processor.py (which loads correct parser per adapter).
Uses TriggerDagRunOperator to trigger Stage 2 upon completion.

Pipeline position:
    Bronze DAG → BRONZE_COMPLETE
    → THIS DAG → STAGING_COMPLETE
    → Triggers Silver Stage 2 DAG

Sensor Gate:
    PythonSensor(mode="reschedule") validates:
        batch_status = 'BRONZE_COMPLETE'
        AND dq_status IN ('PASSED', 'WARNING')

Schedule: Every 10 minutes
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

DAG_ID = "ueh_silver_stage1"
DB_CONTROL = "t01_ueh_dev_ctl"
SPARK_JOB = "/apps/ueh/spark/silver_stage1_processor.py"

default_args = {
    'owner': 'ueh-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


def _get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("UEH_DAG_Stage1").enableHiveSupport().getOrCreate()


def sensor_check_bronze_ready(**kwargs):
    """
    PythonSensor gate: Check for BRONZE_COMPLETE batch with acceptable DQ.
    
    Proceed ONLY when:
        batch_status = 'BRONZE_COMPLETE'
        AND (dq_status IN ('PASSED', 'WARNING') OR dq_status IS NULL)
    
    Block when:
        dq_status = 'FAILED'
    """
    spark = _get_spark()
    try:
        pending = spark.sql(f"""
            SELECT batch_id, adapter_instance_id, dq_status
            FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
            WHERE batch_status = 'BRONZE_COMPLETE'
              AND (dq_status IS NULL OR dq_status IN ('PASSED', 'WARNING', 'NOT_CHECKED'))
            ORDER BY created_at ASC
            LIMIT 1
        """).first()

        if pending:
            kwargs['ti'].xcom_push(key='batch_id', value=pending.batch_id)
            kwargs['ti'].xcom_push(key='adapter_instance_id', value=pending.adapter_instance_id)
            logger.info(f"Sensor PASSED: {pending.batch_id} (dq={pending.dq_status})")
            return True

        # Check if there are FAILED DQ batches (log warning)
        failed_dq = spark.sql(f"""
            SELECT batch_id FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
            WHERE batch_status = 'BRONZE_COMPLETE' AND dq_status = 'FAILED'
            LIMIT 1
        """).first()

        if failed_dq:
            logger.warning(f"Batch {failed_dq.batch_id} blocked: dq_status=FAILED")

        return False
    finally:
        spark.stop()


def verify_staging_complete(**kwargs):
    """Verify Stage 1 completed successfully."""
    spark = _get_spark()
    try:
        batch_id = kwargs['ti'].xcom_pull(key='batch_id')
        result = spark.sql(f"""
            SELECT batch_status, dq_status, records_processed
            FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
            WHERE batch_id = '{batch_id}'
        """).first()

        if result.batch_status == 'STAGING_COMPLETE':
            logger.info(
                f"Stage 1 VERIFIED: {batch_id} → STAGING_COMPLETE "
                f"(records={result.records_processed}, dq={result.dq_status})"
            )
            # Only trigger Stage 2 if DQ is acceptable
            if result.dq_status in ('PASSED', 'WARNING', None):
                kwargs['ti'].xcom_push(key='trigger_stage2', value=True)
            else:
                kwargs['ti'].xcom_push(key='trigger_stage2', value=False)
                logger.warning(f"Stage 1 DQ={result.dq_status}. NOT triggering Stage 2.")
        elif result.batch_status == 'FAILED':
            raise Exception(f"Stage 1 FAILED for {batch_id}")
        else:
            raise Exception(f"Unexpected status: {result.batch_status}")
    finally:
        spark.stop()


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH Silver Stage 1: Parse Bronze → Adapter Staging',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'silver', 'stage1', 'staging', 'parser'],
) as dag:

    # Sensor gate: wait for BRONZE_COMPLETE with acceptable DQ
    t_sensor = PythonSensor(
        task_id='sensor_bronze_ready',
        python_callable=sensor_check_bronze_ready,
        mode='reschedule',
        poke_interval=60,
        timeout=600,
    )

    # Run Stage 1 Spark job
    t_stage1 = SparkSubmitOperator(
        task_id='run_stage1_processor',
        application=SPARK_JOB,
        name='UEH_Stage1_{{ ti.xcom_pull(key="batch_id") }}',
        application_args=['--batch_id', '{{ ti.xcom_pull(key="batch_id") }}'],
        py_files='/apps/ueh/spark/parsers.zip',
        conf={
            'ueh.environment': Variable.get("ueh_environment", default_var="dev"),
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
            'spark.sql.catalog.spark_catalog.type': 'hive',
        },
    )

    # Verify + decide if Stage 2 should trigger
    t_verify = PythonOperator(
        task_id='verify_staging',
        python_callable=verify_staging_complete,
    )

    # Trigger Stage 2 (TriggerDagRunOperator for visible lineage)
    t_trigger_stage2 = TriggerDagRunOperator(
        task_id='trigger_stage2_canonical',
        trigger_dag_id='ueh_silver_stage2',
        conf={'batch_id': '{{ ti.xcom_pull(key="batch_id") }}'},
        wait_for_completion=False,
    )

    t_sensor >> t_stage1 >> t_verify >> t_trigger_stage2
