"""
=============================================================================
UEH Silver Stage 2: Canonical Vulnerability Intelligence
=============================================================================
Merges adapter staging tables into canonical:
    t01_ueh_slv_vulnerability_intel

Sources:
    - t01_ueh_slv_stg_nvd_vulnerability (Stage 1)
    - (Future: t01_ueh_slv_stg_epss, t01_ueh_slv_stg_cisa_kev)

Responsibilities:
    - Cross-source merge on cve_id
    - Canonical field mapping (adapter-specific → standard)
    - Entity resolution (same CVE from multiple sources → one record)
    - Cross-source enrichment (NVD base + EPSS score + CISA KEV flag)

Write strategy: MERGE on cve_id (upsert — latest wins)
Idempotent: DELETE staging records for batch + re-MERGE

Usage:
    spark-submit --conf ueh.environment=dev \
        silver_stage2_vuln_intel.py --batch_id <batch_id>
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, coalesce, when, upper
)
from pyspark.sql.types import DoubleType, BooleanType, TimestampType, DateType, StringType
import argparse
import logging
import traceback

logging.basicConfig(level=logging.INFO, format='[UEH-Stage2-Intel] %(levelname)s: %(message)s')
logger = logging.getLogger("UEH-Stage2-Intel")


def main():
    parser = argparse.ArgumentParser(description="UEH Silver Stage 2: Vuln Intel")
    parser.add_argument("--batch_id", required=True)
    args = parser.parse_args()
    batch_id = args.batch_id

    spark = SparkSession.builder \
        .appName(f"UEH_Silver_Stage2_Intel_{batch_id}") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

    env = spark.conf.get("ueh.environment", "dev")
    db_control = f"t01_ueh_{env}_ctl"
    db_silver = f"t01_ueh_{env}_slv"
    canonical_table = f"{db_silver}.t01_ueh_slv_vulnerability_intel"

    logger.info("=" * 60)
    logger.info(f"Silver Stage 2 (Canonical Intel): batch={batch_id}")
    logger.info("=" * 60)

    try:
        # ─── 1. Read batch context ───────────────────────────────────
        batch = spark.sql(f"""
            SELECT b.batch_id, b.adapter_instance_id, b.batch_status,
                   b.ingestion_date, a.source_system
            FROM {db_control}.t01_ueh_ctl_batch_registry b
            JOIN {db_control}.t01_ueh_ctl_adapter_config a
              ON b.adapter_instance_id = a.adapter_instance_id
            WHERE b.batch_id = '{batch_id}'
        """).first()

        if batch is None:
            raise Exception(f"Batch '{batch_id}' not found!")
        if batch.batch_status != 'STAGING_COMPLETE':
            raise Exception(f"Status is '{batch.batch_status}', expected STAGING_COMPLETE")

        source_system = batch.source_system
        ingestion_date = str(batch.ingestion_date)
        logger.info(f"Source: {source_system}, Date: {ingestion_date}")

        # ─── 2. Read from Staging Table (adapter-specific) ───────────
        if source_system == 'NVD':
            staged_df = spark.sql(f"""
                SELECT * FROM {db_silver}.t01_ueh_slv_stg_nvd_vulnerability
                WHERE batch_id = '{batch_id}'
            """)

            # Map NVD staging → canonical
            canonical_df = staged_df.select(
                col("cve_id"),
                coalesce(col("cvss31_base_score"), col("cvss2_base_score")).alias("cvss_base_score"),
                lit("3.1").alias("cvss_version"),
                upper(coalesce(col("cvss31_severity"), col("cvss2_severity"))).alias("severity"),
                col("description_en").alias("description"),
                col("published_date"),
                col("last_modified_date"),
                col("references_json"),
                col("weaknesses_json"),
                col("configurations_json").alias("affected_products_json"),
                lit(None).cast(DoubleType()).alias("epss_score"),
                lit(None).cast(DoubleType()).alias("epss_percentile"),
                lit(False).cast(BooleanType()).alias("is_in_kev"),
                lit(None).cast(DateType()).alias("kev_date_added"),
                lit(None).cast(DateType()).alias("kev_due_date"),
                lit(False).cast(BooleanType()).alias("is_actively_exploited"),
                lit(f'["{source_system}"]').alias("source_systems_json"),
                lit(batch_id).alias("nvd_batch_id"),
                lit(None).cast(StringType()).alias("epss_batch_id"),
                lit(None).cast(StringType()).alias("cisa_batch_id"),
                current_timestamp().alias("first_seen_in_ueh"),
                current_timestamp().alias("last_updated_in_ueh"),
                lit(ingestion_date).cast("date").alias("ingestion_date"),
                # DQ
                col("dq_has_cvss").alias("dq_has_cvss"),
                lit(False).cast(BooleanType()).alias("dq_has_epss"),
                col("dq_has_description").alias("dq_has_description"),
                lit(None).cast(DoubleType()).alias("dq_completeness_score"),
            )

        # TODO: Add EPSS and CISA_KEV branches here when those parsers exist
        else:
            raise Exception(f"No Stage 2 canonical logic for source_system='{source_system}'")

        # ─── 3. Filter NULL keys ─────────────────────────────────────
        canonical_df = canonical_df.filter("cve_id IS NOT NULL")

        # ─── 4. Align to target schema ───────────────────────────────
        target_cols = [f.name for f in spark.table(canonical_table).schema.fields]
        for c in target_cols:
            if c not in canonical_df.columns:
                canonical_df = canonical_df.withColumn(c, lit(None).cast(StringType()))
        canonical_df = canonical_df.select(*[col(c) for c in target_cols])

        # ─── 5. Cast to match target types ───────────────────────────
        target_schema = spark.table(canonical_table).schema
        for field in target_schema.fields:
            if field.name in canonical_df.columns:
                canonical_df = canonical_df.withColumn(
                    field.name, col(field.name).cast(field.dataType))

        # ─── 6. MERGE into Canonical Table ────────────────────────────
        record_count = canonical_df.count()
        logger.info(f"Merging {record_count} records into {canonical_table}")

        canonical_df.createOrReplaceTempView("canonical_batch")

        all_cols = canonical_df.columns
        non_key = [c for c in all_cols if c != 'cve_id']
        set_clause = ", ".join([f"target.{c} = source.{c}" for c in non_key])
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join([f"source.{c}" for c in all_cols])

        spark.sql(f"""
            MERGE INTO {canonical_table} AS target
            USING canonical_batch AS source
            ON target.cve_id = source.cve_id
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)

        # ─── 7. Update batch_registry → SILVER_COMPLETE ──────────────
        spark.sql(f"""
            UPDATE {db_control}.t01_ueh_ctl_batch_registry
            SET batch_status = 'SILVER_COMPLETE',
                records_processed = {record_count},
                end_time = current_timestamp()
            WHERE batch_id = '{batch_id}'
        """)

        logger.info(f"SUCCESS: {record_count} records → SILVER_COMPLETE")

    except Exception as e:
        logger.error(f"FAILED: {e}")
        traceback.print_exc()
        try:
            spark.sql(f"""
                UPDATE {db_control}.t01_ueh_ctl_batch_registry
                SET batch_status = 'FAILED',
                    failure_reason = '{str(e).replace(chr(39), "")[:500]}',
                    end_time = current_timestamp()
                WHERE batch_id = '{batch_id}'
            """)
        except:
            pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
