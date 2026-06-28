"""
=============================================================================
UEH Platform DAG: Sync Adapter Config → Airflow Variable
=============================================================================
Purpose: Reads active adapters from t01_ueh_ctl_adapter_config and writes
         them into an Airflow Variable so the DAG Factory can read them
         at parse time (without needing Spark at parse time).

Schedule: Every 5 minutes
Writes to: Airflow Variable 'ueh_active_adapters' (JSON array)

Why needed:
    - Airflow parses DAG files every 30s
    - DAG Factory needs to know which adapters exist
    - Can't run Spark at parse time (too slow/heavy)
    - Solution: lightweight sync DAG writes to Variable, factory reads Variable

Flow:
    [Spark reads adapter_config] → [writes JSON to Airflow Variable]
    [DAG Factory reads Variable] → [generates one DAG per adapter]
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)

DAG_ID = "ueh_platform_sync_adapter_config"
DB_CONTROL = "t01_ueh_dev_ctl"

default_args = {
    'owner': 'ueh-platform',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def sync_adapters_to_variable(**kwargs):
    """
    Read all active + schedule_enabled adapters from control table.
    Write as JSON array to Airflow Variable 'ueh_active_adapters'.
    
    The DAG Factory (dag_factory_raw_ingestion.py) reads this Variable
    at parse time to generate per-adapter DAGs.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("UEH_Sync_AdapterConfig") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        adapters_df = spark.sql(f"""
            SELECT 
                adapter_instance_id,
                source_system,
                org_id,
                schedule_cron,
                sla_minutes
            FROM {DB_CONTROL}.t01_ueh_ctl_adapter_config
            WHERE is_active = TRUE
              AND schedule_enabled = TRUE
              AND schedule_cron IS NOT NULL
            ORDER BY source_system, adapter_instance_id
        """)

        adapters = [row.asDict() for row in adapters_df.collect()]

        # Write to Airflow Variable
        Variable.set("ueh_active_adapters", json.dumps(adapters))

        logger.info(f"Synced {len(adapters)} active adapters to Airflow Variable:")
        for a in adapters:
            logger.info(f"  {a['adapter_instance_id']} ({a['source_system']}) → {a['schedule_cron']}")

        # Also store count for monitoring
        Variable.set("ueh_active_adapter_count", str(len(adapters)))

    except Exception as e:
        logger.error(f"Failed to sync adapter config: {e}")
        raise

    finally:
        spark.stop()


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH Platform: Sync adapter_config → Airflow Variable (for DAG Factory)',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'platform', 'config', 'sync'],
    doc_md=__doc__
) as dag:

    sync_task = PythonOperator(
        task_id='sync_adapters_to_variable',
        python_callable=sync_adapters_to_variable,
    )
