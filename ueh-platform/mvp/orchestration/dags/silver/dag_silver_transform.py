"""
=============================================================================
UEH Silver Transform DAG (DAG 3)
=============================================================================
Polls for BRONZE_COMPLETE batches and routes to the correct Silver Spark job.
All Silver jobs run in PARALLEL (no dependency between them).

Architecture:
    ┌─────────────────────────────────────────────────────────┐
    │                    DAG 3: Silver                          │
    │                                                         │
    │  [find_pending]                                          │
    │       │                                                  │
    │       ├──▶ [silver_vuln_intel]   (NVD/EPSS/CISA)         │
    │       │                                                  │
    │       ├──▶ [silver_vuln_findings] (Tenable/Sysdig)       │
    │       │                                                  │
    │       └──▶ [silver_assets]        (ADDM/CMDB)            │
    │                                                         │
    │       All three run PARALLEL (independent batches)       │
    │                                                         │
    │       ├──▶ [verify_intel]                                │
    │       ├──▶ [verify_findings]                             │
    │       └──▶ [verify_assets]                               │
    └─────────────────────────────────────────────────────────┘

Schedule: Every 10 minutes
Database: t01_ueh_dev_ctl
=============================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

DAG_ID = "ueh_dag3_silver_transform"
DB_CONTROL = "t01_ueh_dev_ctl"

# Spark job paths on HDFS
SPARK_JOBS = {
    'vulnerability_intel': '/apps/ueh/spark/silver_vulnerability_intel.py',
    'vulnerability_findings': '/apps/ueh/spark/silver_vulnerability_findings.py',
    'assets': '/apps/ueh/spark/silver_assets.py',
}

# Which source_system routes to which Silver domain
SOURCE_ROUTING = {
    'NVD': 'vulnerability_intel',
    'EPSS': 'vulnerability_intel',
    'CISA_KEV': 'vulnerability_intel',
    'MSRC': 'vulnerability_intel',
    'TENABLE': 'vulnerability_findings',
    'SYSDIG': 'vulnerability_findings',
    'QUALYS': 'vulnerability_findings',
    'FORTIFY': 'vulnerability_findings',
    'BMC_ADDM': 'assets',
    'CMDB': 'assets',
}

default_args = {
    'owner': 'ueh-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("UEH_DAG3_Silver") \
        .enableHiveSupport() \
        .getOrCreate()



def find_pending_batches(**kwargs):
    """
    Find ALL BRONZE_COMPLETE batches (up to 1 per Silver domain).
    Returns batch_ids grouped by Silver domain for parallel processing.
    """
    spark = get_spark()

    pending = spark.sql(f"""
        SELECT 
            b.batch_id, 
            b.adapter_instance_id,
            a.source_system
        FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry b
        JOIN {DB_CONTROL}.t01_ueh_ctl_adapter_config a
          ON b.adapter_instance_id = a.adapter_instance_id
        WHERE b.batch_status = 'BRONZE_COMPLETE'
        ORDER BY b.created_at ASC
    """).collect()

    if not pending:
        logger.info("No BRONZE_COMPLETE batches pending.")
        kwargs['ti'].xcom_push(key='has_work', value=False)
        return

    # Group by Silver domain — pick ONE batch per domain (oldest first)
    domain_batches = {}
    for row in pending:
        domain = SOURCE_ROUTING.get(row.source_system)
        if domain and domain not in domain_batches:
            domain_batches[domain] = {
                'batch_id': row.batch_id,
                'source_system': row.source_system,
                'adapter_instance_id': row.adapter_instance_id,
            }

    logger.info(f"Found batches for {len(domain_batches)} Silver domains: {list(domain_batches.keys())}")

    # Push each domain's batch_id for parallel tasks
    for domain, info in domain_batches.items():
        kwargs['ti'].xcom_push(key=f'{domain}_batch_id', value=info['batch_id'])
        kwargs['ti'].xcom_push(key=f'{domain}_source', value=info['source_system'])
        logger.info(f"  {domain}: batch={info['batch_id']} (source={info['source_system']})")

    kwargs['ti'].xcom_push(key='has_work', value=True)
    kwargs['ti'].xcom_push(key='active_domains', value=list(domain_batches.keys()))


def should_run_domain(domain: str, **kwargs):
    """Check if a specific Silver domain has a pending batch."""
    batch_id = kwargs['ti'].xcom_pull(key=f'{domain}_batch_id')
    has_work = kwargs['ti'].xcom_pull(key='has_work')
    if not has_work or not batch_id:
        logger.info(f"No pending batch for {domain}. Skipping.")
        return False
    return True


def verify_domain(domain: str, **kwargs):
    """Verify Silver completion for a specific domain."""
    batch_id = kwargs['ti'].xcom_pull(key=f'{domain}_batch_id')
    if not batch_id:
        logger.info(f"No batch for {domain} to verify.")
        return

    spark = get_spark()
    result = spark.sql(f"""
        SELECT batch_status, records_processed, failure_reason
        FROM {DB_CONTROL}.t01_ueh_ctl_batch_registry
        WHERE batch_id = '{batch_id}'
    """).first()

    if result.batch_status == 'SILVER_COMPLETE':
        logger.info(f"VERIFIED {domain}: {batch_id} → SILVER_COMPLETE ({result.records_processed} records)")
    elif result.batch_status == 'FAILED':
        logger.error(f"FAILED {domain}: {batch_id} — {result.failure_reason}")
        raise Exception(f"Silver {domain} FAILED: {result.failure_reason}")
    else:
        logger.warning(f"Unexpected status for {domain}: {result.batch_status}")



# ─── Spark Config (shared across all Silver jobs) ─────────────────────────────
SPARK_CONF = {
    'ueh.environment': Variable.get("ueh_environment", default_var="dev"),
    'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
    'spark.sql.catalog.spark_catalog.type': 'hive',
    'spark.driver.memory': '4g',
    'spark.executor.memory': '8g',
    'spark.executor.cores': '4',
    'spark.executor.instances': '2',
}


# ─── DAG Definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH DAG 3: Silver transformation (parallel per domain)',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'silver', 'parallel', 'transform'],
    doc_md=__doc__
) as dag:

    # ─── Start: Find all pending batches ──────────────────────────────
    t_find = PythonOperator(
        task_id='find_pending_batches',
        python_callable=find_pending_batches,
    )

    # ─── PARALLEL BRANCH: Vulnerability Intelligence ──────────────────
    t_check_intel = PythonOperator(
        task_id='check_intel_pending',
        python_callable=should_run_domain,
        op_kwargs={'domain': 'vulnerability_intel'},
    )

    t_run_intel = SparkSubmitOperator(
        task_id='silver_vulnerability_intel',
        application=SPARK_JOBS['vulnerability_intel'],
        name='UEH_Silver_Intel_{{ ti.xcom_pull(key="vulnerability_intel_batch_id") }}',
        application_args=[
            '--batch_id', '{{ ti.xcom_pull(key="vulnerability_intel_batch_id") }}'
        ],
        py_files='/apps/ueh/spark/shared.zip',
        conf=SPARK_CONF,
        verbose=True,
    )

    t_verify_intel = PythonOperator(
        task_id='verify_intel',
        python_callable=verify_domain,
        op_kwargs={'domain': 'vulnerability_intel'},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ─── PARALLEL BRANCH: Vulnerability Findings ──────────────────────
    t_check_findings = PythonOperator(
        task_id='check_findings_pending',
        python_callable=should_run_domain,
        op_kwargs={'domain': 'vulnerability_findings'},
    )

    t_run_findings = SparkSubmitOperator(
        task_id='silver_vulnerability_findings',
        application=SPARK_JOBS['vulnerability_findings'],
        name='UEH_Silver_Findings_{{ ti.xcom_pull(key="vulnerability_findings_batch_id") }}',
        application_args=[
            '--batch_id', '{{ ti.xcom_pull(key="vulnerability_findings_batch_id") }}'
        ],
        py_files='/apps/ueh/spark/shared.zip',
        conf=SPARK_CONF,
        verbose=True,
    )

    t_verify_findings = PythonOperator(
        task_id='verify_findings',
        python_callable=verify_domain,
        op_kwargs={'domain': 'vulnerability_findings'},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ─── PARALLEL BRANCH: Assets ──────────────────────────────────────
    t_check_assets = PythonOperator(
        task_id='check_assets_pending',
        python_callable=should_run_domain,
        op_kwargs={'domain': 'assets'},
    )

    t_run_assets = SparkSubmitOperator(
        task_id='silver_assets',
        application=SPARK_JOBS['assets'],
        name='UEH_Silver_Assets_{{ ti.xcom_pull(key="assets_batch_id") }}',
        application_args=[
            '--batch_id', '{{ ti.xcom_pull(key="assets_batch_id") }}'
        ],
        py_files='/apps/ueh/spark/shared.zip',
        conf=SPARK_CONF,
        verbose=True,
    )

    t_verify_assets = PythonOperator(
        task_id='verify_assets',
        python_callable=verify_domain,
        op_kwargs={'domain': 'assets'},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ─── End: All branches join ───────────────────────────────────────
    t_end = DummyOperator(
        task_id='silver_complete',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ─── PARALLEL DEPENDENCIES ────────────────────────────────────────
    # All three branches run independently after find_pending_batches
    t_find >> t_check_intel >> t_run_intel >> t_verify_intel >> t_end
    t_find >> t_check_findings >> t_run_findings >> t_verify_findings >> t_end
    t_find >> t_check_assets >> t_run_assets >> t_verify_assets >> t_end
