"""
=============================================================================
UEH Gold Compute DAG (DAG 4)
=============================================================================
Runs Gold layer computations DAILY after Silver is complete.
All Gold jobs run in PARALLEL (no dependency between them).

Architecture:
    [check_silver_ready] ──┬──▶ [gold_exposure_summary]
                           ├──▶ [gold_cve_enriched]
                           └──▶ [gold_risk_metrics]    (depends on exposure_summary)

Schedule: Daily at 06:00 UTC (after Silver batches expected complete)
Trigger: Time-based (not polling Silver — assumes Silver runs by 06:00)

Gold tables:
    - gld_exposure_summary  → Enriched findings (JOIN of all 3 Silver tables)
    - gld_cve_enriched      → Fully enriched CVE intelligence
    - gld_risk_metrics      → Aggregated metrics (reads from exposure_summary)
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

DAG_ID = "ueh_dag4_gold_compute"
DB_CONTROL = "t01_ueh_dev_ctl"

SPARK_JOBS = {
    'exposure_summary': '/apps/ueh/spark/gold_exposure_summary.py',
    'cve_enriched': '/apps/ueh/spark/gold_cve_enriched.py',
    'risk_metrics': '/apps/ueh/spark/gold_risk_metrics.py',
}

SPARK_CONF = {
    'ueh.environment': Variable.get("ueh_environment", default_var="dev"),
    'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
    'spark.sql.catalog.spark_catalog.type': 'hive',
    'spark.driver.memory': '4g',
    'spark.executor.memory': '8g',
    'spark.executor.cores': '4',
    'spark.executor.instances': '3',
    'spark.dynamicAllocation.enabled': 'true',
    'spark.dynamicAllocation.maxExecutors': '8',
}

default_args = {
    'owner': 'ueh-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
}


def get_spark():
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName("UEH_DAG4_Gold") \
        .enableHiveSupport() \
        .getOrCreate()


def check_silver_ready(**kwargs):
    """
    Verify that Silver tables have recent data.
    Gold depends on Silver being populated.
    """
    spark = get_spark()
    today = kwargs['ds']

    # Check if vulnerability_intel has data
    intel_count = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM t01_ueh_dev_slv.t01_ueh_slv_vulnerability_intel
    """).first().cnt

    logger.info(f"Silver vulnerability_intel records: {intel_count}")

    if intel_count == 0:
        logger.warning(
            "No Silver vulnerability_intel data. "
            "Gold will produce limited results."
        )

    # Push run_date for Gold jobs
    kwargs['ti'].xcom_push(key='run_date', value=today)
    logger.info(f"Silver check passed. Gold will compute for date={today}")


def log_gold_complete(**kwargs):
    """Log Gold completion."""
    run_date = kwargs['ti'].xcom_pull(key='run_date')
    logger.info(
        f"DAG 4 COMPLETE: All Gold tables computed for {run_date}. "
        f"Data ready for dashboards and chatbot."
    )


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='UEH DAG 4: Gold layer computation (daily enrichment + aggregation)',
    schedule_interval='0 6 * * *',  # Daily at 06:00 UTC
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ueh', 'gold', 'enrichment', 'analytics'],
    doc_md=__doc__
) as dag:

    # ─── Check Silver readiness ───────────────────────────────────────
    t_check = PythonOperator(
        task_id='check_silver_ready',
        python_callable=check_silver_ready,
    )

    # ─── Gold: Exposure Summary (main enrichment table) ───────────────
    t_exposure = SparkSubmitOperator(
        task_id='gold_exposure_summary',
        application=SPARK_JOBS['exposure_summary'],
        name='UEH_Gold_Exposure_{{ ds }}',
        application_args=['--run_date', '{{ ds }}'],
        conf=SPARK_CONF,
        verbose=True,
    )

    # ─── Gold: CVE Enriched (parallel with exposure) ──────────────────
    t_cve = SparkSubmitOperator(
        task_id='gold_cve_enriched',
        application=SPARK_JOBS['cve_enriched'],
        name='UEH_Gold_CVE_{{ ds }}',
        application_args=['--run_date', '{{ ds }}'],
        conf=SPARK_CONF,
        verbose=True,
    )

    # ─── Gold: Risk Metrics (AFTER exposure — reads from it) ──────────
    t_metrics = SparkSubmitOperator(
        task_id='gold_risk_metrics',
        application=SPARK_JOBS['risk_metrics'],
        name='UEH_Gold_Metrics_{{ ds }}',
        application_args=['--run_date', '{{ ds }}'],
        conf=SPARK_CONF,
        verbose=True,
    )

    # ─── End ──────────────────────────────────────────────────────────
    t_done = PythonOperator(
        task_id='gold_complete',
        python_callable=log_gold_complete,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ─── Dependencies ─────────────────────────────────────────────────
    # Exposure + CVE run in PARALLEL after Silver check
    # Risk Metrics runs AFTER Exposure (reads from it)
    t_check >> [t_exposure, t_cve]
    t_exposure >> t_metrics
    [t_metrics, t_cve] >> t_done
