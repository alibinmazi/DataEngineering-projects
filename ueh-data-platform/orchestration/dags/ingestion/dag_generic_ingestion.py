"""
=============================================================================
UEH Generic Ingestion DAG (DAG 1)
=============================================================================
ONE DAG that handles ingestion for ALL active adapters.

How it works:
    1. Query adapter_config for all active + schedule_enabled adapters
    2. For each adapter: check if already ran today
    3. Trigger NiFi (or skip if NiFi is self-scheduled)
    4. Poll batch_registry for RAW_COMPLETE

This replaces per-adapter DAGs (dag_ingest_nvd, dag_ingest_epss, etc.)
with a single dynamic DAG.

Schedule: Every 15 minutes (checks which adapters need processing)
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

DAG_ID = "ueh_generic_ingestion"
DB_CONTROL = "t01_ueh_dev_ctl"

default_args = {
    'owner': 'ueh-platform',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName(f"UEH_DAG1_Generic") \
        .enableHiveSupport() \
        .getOrCreate()


def process_active_adapters(**kwargs):
    """
    Main orchestration logic:
    1. Find all adapters that are active + schedule_enabled
    2. Check which ones haven't run today
    3. For those due: trigger NiFi (or log for self-scheduled)
    4. This task runs periodically — NiFi handles the actual ingestion
    """
    spark = get_spark()
    today = kwargs['ds']

    # Get all active adapters due for execution
    adapters = spark.sql(f"""
        SELECT 
            ac.adapter_instance_id,
            ac.source_system,
            ac.org_id,
            ac.schedule_cron,
            ast.state_status,
            ast.consecutive_failures
        FROM {DB_CONTROL}.t01_ueh_ctl_adapter_config ac
        LEFT JOIN {DB_CONTROL}.t01_ueh_ctl_adapter_state ast
            ON ac.adapter_instance_id = ast.adapter_instance_id
            AND ac.org_id = ast.org_id
        WHERE ac.is_active = TRUE
          AND ac.schedule_enabled = TRUE
          AND (ast.state_status IS NULL OR ast.state_status != 'FAILING')
    """).collect()

    logger.info(f"Found {len(adapters)} active adapters to check.")

    for adapter in adapters:
        instance_id = adapter.adapter_instance_id
        source = adapter.source_system

        # Check if already processed today
        existing = spark.sql(f"""
            SELECT batch_id FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
            WHERE adapter_instance_id = '{instance_id}'
              AND ingestion_date = DATE '{today}'
              AND batch_status IN ('RAW_COMPLETE', 'BRONZE_COMPLETE', 'SILVER_COMPLETE')
            LIMIT 1
        """).first()

        if existing:
            logger.info(f"  {instance_id} ({source}): Already ran today. Skip.")
            continue

        # Check for in-progress
        running = spark.sql(f"""
            SELECT batch_id FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
            WHERE adapter_instance_id = '{instance_id}'
              AND ingestion_date = DATE '{today}'
              AND batch_status IN ('INITIATED', 'RUNNING')
            LIMIT 1
        """).first()

        if running:
            logger.info(f"  {instance_id} ({source}): In progress. Skip.")
            continue

        # Adapter needs processing — NiFi should handle it
        # In config-driven model: NiFi reads adapter_config on its own schedule
        # This DAG just monitors/validates
        logger.info(f"  {instance_id} ({source}): DUE for ingestion. NiFi should pick up.")

    logger.info("Generic ingestion check complete.")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH DAG 1: Monitor ingestion status across all adapters',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'platform', 'ingestion', 'generic'],
) as dag:

    check_adapters = PythonOperator(
        task_id='process_active_adapters',
        python_callable=process_active_adapters,
    )
