"""
=============================================================================
UEH Silver Stage 1: Adapter Staging Processor
=============================================================================
Generic orchestrator that:
    1. Reads batch context (which adapter, which batch)
    2. Loads the correct parser class for that adapter
    3. Reads Bronze payload_json records
    4. Calls parser.parse() → typed DataFrame
    5. Writes to adapter-specific staging table
    6. Computes DQ summary → updates batch_registry.dq_status
    7. Updates batch_registry → STAGING_COMPLETE

This job is IDEMPOTENT:
    - Re-running same batch_id deletes previous staging output + rewrites
    - Uses DELETE + INSERT (not APPEND) for idempotency

Usage:
    spark-submit --conf ueh.environment=dev \
        silver_stage1_processor.py --batch_id <batch_id>
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, count, sum as spark_sum
import argparse
import importlib
import logging
import traceback
import json

logging.basicConfig(level=logging.INFO, format='[UEH-Stage1] %(levelname)s: %(message)s')
logger = logging.getLogger("UEH-Stage1")

# ─── Source System → Parser Module + Target Staging Table ─────────────────────
ADAPTER_REGISTRY = {
    'NVD': {
        'parser_module': 'parsers.nvd_parser_v1',
        'staging_table': 't01_ueh_slv_stg_nvd_vulnerability',
    },
    'TENABLE': {
        'parser_module': 'parsers.tenable_parser_v1',
        'staging_table': 't01_ueh_slv_stg_tenable_finding',
    },
    'BMC_ADDM': {
        'parser_module': 'parsers.addm_parser_v1',
        'staging_table': 't01_ueh_slv_stg_addm_asset',
    },
    # Future adapters:
    # 'SYSDIG': { 'parser_module': 'parsers.sysdig_parser_v1', ... },
    # 'EPSS': { 'parser_module': 'parsers.epss_parser_v1', ... },
}


def main():
    parser = argparse.ArgumentParser(description="UEH Silver Stage 1 Processor")
    parser.add_argument("--batch_id", required=True)
    args = parser.parse_args()
    batch_id = args.batch_id

    spark = SparkSession.builder \
        .appName(f"UEH_Silver_Stage1_{batch_id}") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

    env = spark.conf.get("ueh.environment", "dev")
    db_control = f"t01_ueh_{env}_ctl"
    db_bronze = f"t01_ueh_{env}_brz"
    db_silver = f"t01_ueh_{env}_slv"

    logger.info("=" * 60)
    logger.info(f"Silver Stage 1: batch={batch_id}, env={env}")
    logger.info("=" * 60)

    try:
        # ─── 1. Read Batch Context ───────────────────────────────────
        batch = spark.sql(f"""
            SELECT b.batch_id, b.adapter_instance_id, b.batch_status,
                   b.ingestion_date, b.records_processed, b.org_id,
                   a.source_system
            FROM {db_control}.t01_ueh_ctl_batch_registry b
            JOIN {db_control}.t01_ueh_ctl_adapter_config a
              ON b.adapter_instance_id = a.adapter_instance_id
            WHERE b.batch_id = '{batch_id}'
        """).first()

        if batch is None:
            raise Exception(f"Batch '{batch_id}' not found!")
        if batch.batch_status != 'BRONZE_COMPLETE':
            raise Exception(f"Status is '{batch.batch_status}', expected BRONZE_COMPLETE")

        source_system = batch.source_system
        adapter_instance_id = batch.adapter_instance_id
        ingestion_date = str(batch.ingestion_date)

        # ─── 2. Get Parser + Target Table ────────────────────────────
        if source_system not in ADAPTER_REGISTRY:
            raise Exception(
                f"No parser registered for source_system='{source_system}'. "
                f"Add to ADAPTER_REGISTRY in silver_stage1_processor.py"
            )

        adapter_info = ADAPTER_REGISTRY[source_system]
        parser_module_name = adapter_info['parser_module']
        staging_table = f"{db_silver}.{adapter_info['staging_table']}"

        logger.info(f"Source: {source_system}")
        logger.info(f"Parser: {parser_module_name}")
        logger.info(f"Target: {staging_table}")

        # ─── 3. Load Parser Module ───────────────────────────────────
        parser_module = importlib.import_module(parser_module_name)
        logger.info(f"Parser loaded: {parser_module_name}")

        # ─── 4. Read Bronze Records ─────────────────────────────────
        bronze_table = f"{db_bronze}.t01_ueh_brz_{source_system.lower()}_raw"
        bronze_df = spark.sql(f"""
            SELECT payload_json, source_record_id
            FROM {bronze_table}
            WHERE batch_id = '{batch_id}'
        """)

        bronze_count = bronze_df.count()
        logger.info(f"Bronze records: {bronze_count}")

        if bronze_count == 0:
            logger.warning("No Bronze records. Marking STAGING_COMPLETE with 0.")
            _update_status(spark, db_control, batch_id, 'STAGING_COMPLETE', 0, 'PASSED')
            return

        # ─── 5. Call Parser ──────────────────────────────────────────
        staged_df = parser_module.parse(
            bronze_df=bronze_df,
            batch_id=batch_id,
            adapter_instance_id=adapter_instance_id,
            ingestion_date=ingestion_date
        )

        # ─── 6. Idempotency: Delete existing records for this batch ──
        spark.sql(f"""
            DELETE FROM {staging_table}
            WHERE batch_id = '{batch_id}'
        """)
        logger.info(f"Idempotency: cleared previous staging for batch {batch_id}")

        # ─── 7. Write to Staging Table ───────────────────────────────
        record_count = staged_df.count()
        logger.info(f"Writing {record_count} staged records to {staging_table}")
        staged_df.writeTo(staging_table).append()

        # ─── 8. Compute DQ Summary ──────────────────────────────────
        dq_status, dq_details = _compute_dq_summary(staged_df, source_system)
        logger.info(f"DQ Status: {dq_status}")

        # ─── 9. Update batch_registry ────────────────────────────────
        _update_status(spark, db_control, batch_id,
                       'STAGING_COMPLETE', record_count, dq_status, dq_details)

        logger.info("=" * 60)
        logger.info(f"SUCCESS: {record_count} records → STAGING_COMPLETE (dq={dq_status})")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"FAILED: {e}")
        traceback.print_exc()
        try:
            _update_status(spark, db_control, batch_id, 'FAILED', 0, 'FAILED', str(e))
        except:
            pass
        raise
    finally:
        spark.stop()


def _compute_dq_summary(df, source_system):
    """Compute DQ summary from DQ flags in staged DataFrame."""
    total = df.count()
    if total == 0:
        return 'PASSED', '{}'

    # Find all dq_ columns
    dq_cols = [c for c in df.columns if c.startswith('dq_')]

    dq_details = {}
    failed_count = 0

    for dq_col in dq_cols:
        # Count FALSEs (failures)
        failures = df.where(col(dq_col) == False).count()
        failure_rate = failures / total
        dq_details[dq_col] = {
            'failures': failures,
            'total': total,
            'failure_rate': round(failure_rate, 4)
        }
        if failure_rate > 0.1:  # >10% failure = critical
            failed_count += 1

    # Determine overall DQ status
    if failed_count == 0:
        overall = 'PASSED'
    elif any(d['failure_rate'] > 0.5 for d in dq_details.values()):
        overall = 'FAILED'  # >50% failure on any check = FAILED
    else:
        overall = 'WARNING'

    return overall, json.dumps(dq_details)


def _update_status(spark, db_control, batch_id, status, records,
                   dq_status='NOT_CHECKED', dq_details=None):
    """Update batch_registry with status and DQ results."""
    sets = [
        f"batch_status = '{status}'",
        f"records_processed = {records}",
        f"dq_status = '{dq_status}'",
        "end_time = current_timestamp()"
    ]
    if dq_details:
        safe_details = str(dq_details).replace("'", "''")[:2000]
        sets.append(f"dq_details_json = '{safe_details}'")

    spark.sql(f"""
        UPDATE {db_control}.t01_ueh_ctl_batch_registry
        SET {', '.join(sets)}
        WHERE batch_id = '{batch_id}'
    """)


if __name__ == "__main__":
    main()
