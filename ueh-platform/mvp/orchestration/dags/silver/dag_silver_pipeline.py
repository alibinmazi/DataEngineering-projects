"""
=============================================================================
UEH Silver Pipeline DAG (Single DAG — Two Tasks)
=============================================================================
ONE DAG that handles the complete Silver transformation:
    Task 1: Stage 1 (Adapter Staging) — parse Bronze → typed staging table
    Task 2: Stage 2 (Canonical)       — staging → MERGE into canonical Silver

Pipeline:
    [sensor_bronze_ready] → [run_stage1_staging] → [check_stage1_dq] → [run_stage2_canonical] → [verify]

Sensor Gate:
    PythonSensor(mode="reschedule"): batch_status=BRONZE_COMPLETE AND dq OK

DQ Gate (between tasks):
    Stage 1 dq_status: PASSED/WARNING → proceed. FAILED → block.

Schedule: Every 10 minutes
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

DAG_ID = "ueh_silver_pipeline"
DB_CONTROL = "t01_ueh_dev_ctl"
STAGE1_JOB = "/apps/ueh/spark/silver_stage1_processor.py"

STAGE2_JOBS = {
    'NVD': '/apps/ueh/spark/silver_stage2_vuln_intel.py',
    'EPSS': '/apps/ueh/spark/silver_stage2_vuln_intel.py',
    'CISA_KEV': '/apps/ueh/spark/silver_stage2_vuln_intel.py',
    'TENABLE': '/apps/ueh/spark/silver_stage2_vuln_findings.py',
    'SYSDIG': '/apps/ueh/spark/silver_stage2_vuln_findings.py',
    'BMC_ADDM': '/apps/ueh/spark/silver_stage2_assets.py',
    'CMDB': '/apps/ueh/spark/silver_stage2_assets.py',
}

SPARK_CONF = {
    'ueh.environment': Variable.get("ueh_environment", default_var="dev"),
    'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
    'spark.sql.catalog.spark_catalog.type': 'hive',
    'spark.driver.memory': '4g',
    'spark.executor.memory': '8g',
}

default_args = {'owner': 'ueh-platform', 'retries': 2, 'retry_delay': timedelta(minutes=3)}


def _get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("UEH_Silver_Pipeline").enableHiveSupport().getOrCreate()


def sensor_bronze_ready(**kwargs):
    """PythonSensor: Find BRONZE_COMPLETE batch with acceptable DQ."""
    spark = _get_spark()
    try:
        pending = spark.sql(f"""
            SELECT b.batch_id, b.adapter_instance_id, b.dq_status, a.source_system
            FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry b
            JOIN {DB_CONTROL}.t01_ueh_ctl_adapter_config a
              ON b.adapter_instance_id = a.adapter_instance_id
            WHERE b.batch_status = 'BRONZE_COMPLETE'
              AND (b.dq_status IS NULL OR b.dq_status IN ('PASSED','WARNING','NOT_CHECKED'))
            ORDER BY b.created_at ASC LIMIT 1
        """).first()
        if pending:
            stage2_job = STAGE2_JOBS.get(pending.source_system)
            if not stage2_job:
                logger.error(f"No Stage 2 job for {pending.source_system}")
                return False
            kwargs['ti'].xcom_push(key='batch_id', value=pending.batch_id)
            kwargs['ti'].xcom_push(key='source_system', value=pending.source_system)
            kwargs['ti'].xcom_push(key='stage2_job', value=stage2_job)
            logger.info(f"Sensor PASSED: {pending.batch_id} ({pending.source_system})")
            return True
        return False
    finally:
        spark.stop()


def check_stage1_dq(**kwargs):
    """DQ Gate: After Stage 1, check dq_status. Block if FAILED."""
    spark = _get_spark()
    try:
        batch_id = kwargs['ti'].xcom_pull(key='batch_id')
        result = spark.sql(f"""
            SELECT batch_status, dq_status, records_processed
            FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
            WHERE batch_id = '{batch_id}'
        """).first()
        if result.batch_status == 'FAILED':
            raise Exception(f"Stage 1 FAILED for {batch_id}")
        if result.batch_status != 'STAGING_COMPLETE':
            raise Exception(f"Expected STAGING_COMPLETE, got {result.batch_status}")
        dq = result.dq_status or 'NOT_CHECKED'
        if dq == 'FAILED':
            logger.error(f"DQ BLOCKED: {batch_id} dq_status=FAILED. Stage 2 skipped.")
            return False
        logger.info(f"DQ gate passed: {batch_id} dq={dq}, records={result.records_processed}")
        return True
    finally:
        spark.stop()


def verify_silver_complete(**kwargs):
    """Verify batch reached SILVER_COMPLETE."""
    spark = _get_spark()
    try:
        batch_id = kwargs['ti'].xcom_pull(key='batch_id')
        source = kwargs['ti'].xcom_pull(key='source_system')
        result = spark.sql(f"""
            SELECT batch_status, records_processed, failure_reason
            FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry WHERE batch_id = '{batch_id}'
        """).first()
        if result.batch_status == 'FAILED':
            raise Exception(f"Stage 2 FAILED: {batch_id} — {result.failure_reason}")
        if result.batch_status != 'SILVER_COMPLETE':
            raise Exception(f"Expected SILVER_COMPLETE, got {result.batch_status}")
        logger.info(f"SILVER COMPLETE: {batch_id} ({source}) records={result.records_processed}")
    finally:
        spark.stop()


with DAG(
    dag_id=DAG_ID, default_args=default_args,
    description='UEH Silver: Stage 1 (staging) → DQ gate → Stage 2 (canonical)',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2026, 6, 1), catchup=False, max_active_runs=1,
    tags=['ueh', 'silver', 'pipeline'],
) as dag:

    t_sensor = PythonSensor(
        task_id='sensor_bronze_ready', python_callable=sensor_bronze_ready,
        mode='reschedule', poke_interval=60, timeout=600,
    )
    t_stage1 = SparkSubmitOperator(
        task_id='run_stage1_staging', application=STAGE1_JOB,
        name='UEH_Stage1_{{ ti.xcom_pull(key="source_system") }}_{{ ti.xcom_pull(key="batch_id") }}',
        application_args=['--batch_id', '{{ ti.xcom_pull(key="batch_id") }}'],
        py_files='/apps/ueh/spark/parsers.zip', conf=SPARK_CONF, verbose=True,
    )
    t_dq_gate = ShortCircuitOperator(
        task_id='check_stage1_dq', python_callable=check_stage1_dq,
    )
    t_stage2 = SparkSubmitOperator(
        task_id='run_stage2_canonical',
        application='{{ ti.xcom_pull(key="stage2_job") }}',
        name='UEH_Stage2_{{ ti.xcom_pull(key="source_system") }}_{{ ti.xcom_pull(key="batch_id") }}',
        application_args=['--batch_id', '{{ ti.xcom_pull(key="batch_id") }}'],
        conf=SPARK_CONF, verbose=True,
    )
    t_verify = PythonOperator(
        task_id='verify_silver_complete', python_callable=verify_silver_complete,
    )

    t_sensor >> t_stage1 >> t_dq_gate >> t_stage2 >> t_verify
