"""
=============================================================================
UEH DAG Factory: Raw Ingestion
=============================================================================
ONE Python file that generates a SEPARATE DAG per active adapter.
Each adapter gets its OWN dag_id + OWN schedule from adapter_config.

Example output (3 adapters configured):
    ueh_raw_nvd__nvd_prod_01            schedule='0 3 * * *'
    ueh_raw_tenable__tenable_prod_us_01 schedule='0 */4 * * *'
    ueh_raw_bmc_addm__addm_prod_01      schedule='0 2 * * *'

How it works:
    1. Reads active adapters from Airflow Variable (synced by platform DAG)
    2. For each adapter: creates a DAG with preflight → trigger_nifi → wait_raw_complete
    3. Each DAG is independent (different schedule, independent failures)

Adding new adapter:
    1. INSERT into adapter_config (via UI or SQL)
    2. Sync DAG updates Airflow Variable
    3. Next Airflow parse → new DAG appears automatically
    4. ZERO code deployment needed
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import time
import logging
import requests

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
DB_CONTROL = "t01_ueh_dev_ctl"

default_args = {
    'owner': 'ueh-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


# ─── Read Active Adapters from Airflow Variable ──────────────────────────────
def get_active_adapters():
    """
    Read active adapters from Airflow Variable.
    This Variable is populated by dag_sync_adapter_config.py every 5 minutes.
    
    Returns list of dicts:
    [
        {"adapter_instance_id": "nvd_prod_01", "source_system": "NVD", 
         "org_id": "default_org", "schedule_cron": "0 3 * * *", "sla_minutes": 60},
        ...
    ]
    """
    try:
        adapters_json = Variable.get("ueh_active_adapters", default_var="[]")
        return json.loads(adapters_json)
    except Exception as e:
        logger.error(f"Failed to read ueh_active_adapters variable: {e}")
        return []


# ─── DAG Factory Function ─────────────────────────────────────────────────────
def create_raw_ingestion_dag(adapter: dict) -> DAG:
    """
    Create a complete Raw Ingestion DAG for one adapter instance.
    
    Tasks:
        1. preflight_check → Is adapter active/healthy? Already ran today?
        2. trigger_nifi → Start NiFi ingestion for this adapter
        3. wait_raw_complete → Poll batch_registry until RAW_COMPLETE
    """
    adapter_instance_id = adapter['adapter_instance_id']
    source_system = adapter['source_system']
    schedule = adapter.get('schedule_cron', '@daily')
    org_id = adapter.get('org_id', 'default_org')
    sla_minutes = adapter.get('sla_minutes', 60)

    dag_id = f"ueh_raw_{source_system.lower()}__{adapter_instance_id}"

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'UEH Raw: {source_system} ({adapter_instance_id})',
        schedule_interval=schedule,
        start_date=datetime(2026, 6, 1),
        catchup=False,
        max_active_runs=1,
        tags=['ueh', 'raw', source_system.lower(), adapter_instance_id],
    )

    # ─── Task: Preflight Check ────────────────────────────────────────
    def preflight(**kwargs):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName(f"UEH_preflight_{adapter_instance_id}") \
            .enableHiveSupport().getOrCreate()
        try:
            # Check adapter state
            state = spark.sql(f"""
                SELECT state_status, consecutive_failures
                FROM {DB_CONTROL}.t01_ueh_ctl_adapter_state
                WHERE adapter_instance_id = '{adapter_instance_id}'
                  AND org_id = '{org_id}'
            """).first()

            if state and state.state_status in ('FAILING', 'DISABLED'):
                logger.warning(
                    f"{adapter_instance_id} is {state.state_status} "
                    f"(failures={state.consecutive_failures}). SKIP."
                )
                return False

            # Check if already ran for this execution date
            today = kwargs['ds']
            existing = spark.sql(f"""
                SELECT batch_id FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
                WHERE adapter_instance_id = '{adapter_instance_id}'
                  AND org_id = '{org_id}'
                  AND ingestion_date = DATE '{today}'
                  AND batch_status IN ('RAW_COMPLETE', 'BRONZE_COMPLETE', 'SILVER_COMPLETE')
                LIMIT 1
            """).first()

            if existing:
                logger.info(f"{adapter_instance_id} already ran for {today}. SKIP.")
                return False

            logger.info(f"Preflight PASSED for {adapter_instance_id}")
            return True
        finally:
            spark.stop()

    # ─── Task: Trigger NiFi ───────────────────────────────────────────
    def trigger_nifi(**kwargs):
        nifi_url = Variable.get("ueh_nifi_base_url", default_var="http://nifi:8080")

        try:
            # Option A: Trigger specific processor group
            pg_id = Variable.get(
                f"ueh_nifi_pg_{source_system.lower()}",
                default_var=Variable.get("ueh_nifi_pg_default", default_var="")
            )
            if pg_id:
                response = requests.put(
                    f"{nifi_url}/nifi-api/flow/process-groups/{pg_id}",
                    json={"id": pg_id, "state": "RUNNING"},
                    timeout=30
                )
                response.raise_for_status()
                logger.info(f"NiFi triggered for {adapter_instance_id} (pg={pg_id})")
            else:
                logger.info(
                    f"No NiFi PG configured for {source_system}. "
                    f"Assuming NiFi is self-scheduled."
                )
        except Exception as e:
            logger.warning(f"NiFi trigger attempt: {e}. Continuing to poll.")

        kwargs['ti'].xcom_push(key='trigger_time', value=datetime.utcnow().isoformat())

    # ─── Task: Wait for RAW_COMPLETE ──────────────────────────────────
    def wait_raw_complete(**kwargs):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName(f"UEH_wait_{adapter_instance_id}") \
            .enableHiveSupport().getOrCreate()
        try:
            today = kwargs['ds']
            poll_interval = 60
            max_wait = int(sla_minutes) * 60

            logger.info(
                f"Polling RAW_COMPLETE: adapter={adapter_instance_id}, "
                f"date={today}, timeout={max_wait}s"
            )

            elapsed = 0
            while elapsed < max_wait:
                result = spark.sql(f"""
                    SELECT batch_id, records_expected, bronze_path
                    FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
                    WHERE adapter_instance_id = '{adapter_instance_id}'
                      AND org_id = '{org_id}'
                      AND ingestion_date = DATE '{today}'
                      AND batch_status = 'RAW_COMPLETE'
                    ORDER BY created_at DESC LIMIT 1
                """).first()

                if result:
                    logger.info(
                        f"RAW_COMPLETE: batch={result.batch_id}, "
                        f"records={result.records_expected}"
                    )
                    kwargs['ti'].xcom_push(key='batch_id', value=result.batch_id)
                    return

                # Check for FAILED
                failed = spark.sql(f"""
                    SELECT batch_id, failure_reason
                    FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
                    WHERE adapter_instance_id = '{adapter_instance_id}'
                      AND org_id = '{org_id}'
                      AND ingestion_date = DATE '{today}'
                      AND batch_status = 'FAILED'
                    ORDER BY created_at DESC LIMIT 1
                """).first()

                if failed:
                    raise Exception(
                        f"Ingestion FAILED: {failed.batch_id} — {failed.failure_reason}"
                    )

                time.sleep(poll_interval)
                elapsed += poll_interval

                if elapsed % 300 == 0:
                    logger.info(f"Waiting... ({elapsed}s / {max_wait}s)")

            raise Exception(
                f"TIMEOUT: RAW_COMPLETE not achieved within {sla_minutes} min "
                f"for {adapter_instance_id}"
            )
        finally:
            spark.stop()

    # ─── Build DAG Tasks ──────────────────────────────────────────────
    with dag:
        t1 = ShortCircuitOperator(
            task_id='preflight_check',
            python_callable=preflight,
        )
        t2 = PythonOperator(
            task_id='trigger_nifi',
            python_callable=trigger_nifi,
        )
        t3 = PythonOperator(
            task_id='wait_raw_complete',
            python_callable=wait_raw_complete,
            execution_timeout=timedelta(minutes=sla_minutes + 30),
        )
        t1 >> t2 >> t3

    return dag


# ─── Generate DAGs (Airflow parse time) ──────────────────────────────────────
adapters = get_active_adapters()

for adapter in adapters:
    generated_dag = create_raw_ingestion_dag(adapter)
    globals()[generated_dag.dag_id] = generated_dag

# Log for debugging
if adapters:
    logger.info(f"DAG Factory: Generated {len(adapters)} raw ingestion DAGs")
else:
    logger.warning("DAG Factory: No active adapters found in ueh_active_adapters variable")
