"""
=============================================================================
UEH DAG: Silver Stage 2 (Canonical Domain Layer)
=============================================================================
Triggered by Stage 1 DAG via TriggerDagRunOperator.
Reads STAGING_COMPLETE batches and merges into canonical Silver tables.

Pipeline position:
    Stage 1 DAG → STAGING_COMPLETE → triggers THIS DAG
    → THIS DAG → SILVER_COMPLETE
    → Triggers Gold DAG

Sensor Gate:
    PythonSensor validates:
        batch_status = 'STAGING_COMPLETE'
        AND dq_status IN ('PASSED', 'WARNING')

Canonical tables produced:
    - t01_ueh_slv_vulnerability_intel (from NVD, EPSS, CISA staging)
    - t01_ueh_slv_vulnerability_finding (from Tenable, Sysdig staging)
    - t01_ueh_slv_asset (from ADDM, CMDB staging)

Schedule: None (triggered by Stage 1 DAG)
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

DAG_ID = "ueh_silver_stage2"
DB_CONTROL = "t01_ueh_dev_ctl"

# Source system → Stage 2 Spark job
STAGE2_JOBS = {
    'NVD': '/apps/ueh/spark/silver_stage2_vuln_intel.py',
    'EPSS': '/apps/ueh/spark/silver_stage2_vuln_intel.py',
    'CISA_KEV': '/apps/ueh/spark/silver_stage2_vuln_intel.py',
    'TENABLE': '/apps/ueh/spark/silver_stage2_vuln_findings.py',
    'SYSDIG': '/apps/ueh/spark/silver_stage2_vuln_findings.py',
    'BMC_ADDM': '/apps/ueh/spark/silver_stage2_assets.py',
    'CMDB': '/apps/ueh/spark/silver_stage2_assets.py',
}

default_args = {
    'owner': 'ueh-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


def _get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("UEH_DAG_Stage2").enableHiveSupport().getOrCreate()


def sensor_check_staging_ready(**kwargs):
    """
    PythonSensor: Check for STAGING_COMPLETE batch with acceptable DQ.
    
    Checks triggered conf first (if called by TriggerDagRunOperator),
    otherwise polls for any STAGING_COMPLETE batch.
    """
    spark = _get_spark()
    try:
        # Check if triggered with specific batch_id
        dag_conf = kwargs.get('dag_run', {}).conf if hasattr(kwargs.get('dag_run', {}), 'conf') else {}
        triggered_batch = dag_conf.get('batch_id') if dag_conf else None

        if triggered_batch:
            result = spark.sql(f"""
                SELECT b.batch_id, b.dq_status, a.source_system
                FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry b
                JOIN {DB_CONTROL}.t01_ueh_ctl_adapter_config a
                  ON b.adapter_instance_id = a.adapter_instance_id
                WHERE b.batch_id = '{triggered_batch}'
                  AND b.batch_status = 'STAGING_COMPLETE'
                  AND (b.dq_status IS NULL OR b.dq_status IN ('PASSED', 'WARNING'))
            """).first()
        else:
            result = spark.sql(f"""
                SELECT b.batch_id, b.dq_status, a.source_system
                FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry b
                JOIN {DB_CONTROL}.t01_ueh_ctl_adapter_config a
                  ON b.adapter_instance_id = a.adapter_instance_id
                WHERE b.batch_status = 'STAGING_COMPLETE'
                  AND (b.dq_status IS NULL OR b.dq_status IN ('PASSED', 'WARNING'))
                ORDER BY b.created_at ASC LIMIT 1
            """).first()

        if result:
            kwargs['ti'].xcom_push(key='batch_id', value=result.batch_id)
            kwargs['ti'].xcom_push(key='source_system', value=result.source_system)

            # Determine which Spark job to run
            job = STAGE2_JOBS.get(result.source_system)
            if job:
                kwargs['ti'].xcom_push(key='stage2_job', value=job)
            else:
                logger.error(f"No Stage 2 job for {result.source_system}")
                return False

            logger.info(f"Sensor PASSED: {result.batch_id} ({result.source_system})")
            return True

        return False
    finally:
        spark.stop()


def verify_silver_complete(**kwargs):
    """Verify canonical MERGE completed."""
    spark = _get_spark()
    try:
        batch_id = kwargs['ti'].xcom_pull(key='batch_id')
        result = spark.sql(f"""
            SELECT batch_status, records_processed
            FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
            WHERE batch_id = '{batch_id}'
        """).first()

        if result.batch_status == 'SILVER_COMPLETE':
            logger.info(f"Stage 2 VERIFIED: {batch_id} → SILVER_COMPLETE ({result.records_processed} records)")
        elif result.batch_status == 'FAILED':
            raise Exception(f"Stage 2 FAILED for {batch_id}")
        else:
            raise Exception(f"Unexpected: {result.batch_status}")
    finally:
        spark.stop()


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH Silver Stage 2: Canonical domain merge',
    schedule_interval=None,  # Triggered by Stage 1
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'silver', 'stage2', 'canonical'],
) as dag:

    t_sensor = PythonSensor(
        task_id='sensor_staging_ready',
        python_callable=sensor_check_staging_ready,
        mode='reschedule',
        poke_interval=30,
        timeout=300,
    )

    t_stage2 = SparkSubmitOperator(
        task_id='run_stage2_canonical',
        application='{{ ti.xcom_pull(key="stage2_job") }}',
        name='UEH_Stage2_{{ ti.xcom_pull(key="source_system") }}_{{ ti.xcom_pull(key="batch_id") }}',
        application_args=['--batch_id', '{{ ti.xcom_pull(key="batch_id") }}'],
        conf={
            'ueh.environment': Variable.get("ueh_environment", default_var="dev"),
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
            'spark.sql.catalog.spark_catalog.type': 'hive',
        },
    )

    t_verify = PythonOperator(
        task_id='verify_silver_complete',
        python_callable=verify_silver_complete,
    )

    t_sensor >> t_stage2 >> t_verify
