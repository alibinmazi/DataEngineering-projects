"""
=============================================================================
UEH Gold: CVE Enriched (Complete Intelligence View)
=============================================================================
Target table: t01_ueh_gld_cve_enriched
Sources: slv_vulnerability_intel + exposure counts from slv_vulnerability_findings
Strategy: OVERWRITE partition daily

Provides a single fully-enriched CVE record with:
    - NVD base data (description, CVSS, references)
    - EPSS exploit probability
    - CISA KEV status + deadline
    - Org exposure count (how many assets affected)
    - UEH priority score + tier

Used by: Chatbot, CVE lookup API, analyst investigation

Usage:
    spark-submit --conf ueh.environment=dev \
        gold_cve_enriched.py --run_date 2026-06-20
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, datediff,
    count, countDistinct, min as spark_min, max as spark_max,
    when, coalesce, greatest
)
import argparse
import logging
import traceback
from pyspark.sql.functions import least

logging.basicConfig(level=logging.INFO, format='[UEH-Gold-CVE] %(levelname)s: %(message)s')
logger = logging.getLogger("UEH-Gold-CVE")


def main():
    parser = argparse.ArgumentParser(description="UEH Gold: CVE Enriched")
    parser.add_argument("--run_date", required=False)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("UEH_Gold_CVEEnriched") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

    env = spark.conf.get("ueh.environment", "dev")
    db_silver = f"t01_ueh_{env}_slv"
    db_gold = f"t01_ueh_{env}_gld"
    gold_table = f"{db_gold}.t01_ueh_gld_cve_enriched"

    run_date = args.run_date or str(spark.sql("SELECT current_date()").first()[0])

    logger.info("=" * 60)
    logger.info(f"Gold CVE Enriched: run_date={run_date}")
    logger.info("=" * 60)

    try:
        # ─── 1. Read all CVE intelligence ─────────────────────────────
        intel_df = spark.sql(f"""
            SELECT
                cve_id, description, severity, cvss_base_score, cvss_version,
                published_date, last_modified_date, references_json,
                affected_products_json, weaknesses_json,
                epss_score, epss_percentile,
                is_in_kev, kev_date_added, kev_due_date, is_actively_exploited,
                source_systems_json
            FROM {db_silver}.t01_ueh_slv_vulnerability_intel
        """)

        intel_count = intel_df.count()
        logger.info(f"CVE intel records: {intel_count}")

        # ─── 2. Compute org exposure per CVE ──────────────────────────
        exposure_df = spark.sql(f"""
            SELECT
                cve_id,
                COUNT(*) AS total_affected_assets,
                SUM(CASE WHEN asset_id IN (
                    SELECT asset_id FROM {db_silver}.t01_ueh_slv_assets
                    WHERE criticality = 'CRITICAL'
                ) THEN 1 ELSE 0 END) AS critical_assets_affected,
                SUM(CASE WHEN asset_id IN (
                    SELECT asset_id FROM {db_silver}.t01_ueh_slv_assets
                    WHERE environment = 'PRODUCTION'
                ) THEN 1 ELSE 0 END) AS production_assets_affected,
                MIN(first_seen) AS first_detected_in_org,
                MAX(last_seen) AS last_detected_in_org
            FROM {db_silver}.t01_ueh_slv_vulnerability_findings
            WHERE cve_id IS NOT NULL
              AND status IN ('OPEN', 'REOPENED')
            GROUP BY cve_id
        """)

        logger.info(f"CVEs with org exposure: {exposure_df.count()}")

        # ─── 3. JOIN intel + exposure ─────────────────────────────────
        enriched_df = intel_df.join(
            exposure_df, intel_df.cve_id == exposure_df.cve_id, "left"
        ).drop(exposure_df.cve_id)

        # Fill nulls for CVEs not found in org
        enriched_df = enriched_df \
            .withColumn("total_affected_assets",
                        coalesce(col("total_affected_assets"), lit(0))) \
            .withColumn("critical_assets_affected",
                        coalesce(col("critical_assets_affected"), lit(0))) \
            .withColumn("production_assets_affected",
                        coalesce(col("production_assets_affected"), lit(0)))

        # ─── 4. Compute derived fields ────────────────────────────────
        # Days in org
        enriched_df = enriched_df.withColumn("days_in_org",
            when(col("first_detected_in_org").isNotNull(),
                 datediff(current_date(), col("first_detected_in_org")).cast("int"))
            .otherwise(lit(None)))

        # Days until KEV deadline
        enriched_df = enriched_df.withColumn("days_until_kev_deadline",
            when(col("kev_due_date").isNotNull(),
                 datediff(col("kev_due_date"), current_date()).cast("int"))
            .otherwise(lit(None)))

        # Exploit likelihood category
        enriched_df = enriched_df.withColumn("exploit_likelihood",
            when(col("epss_score") > 0.9, lit("VERY_HIGH"))
            .when(col("epss_score") > 0.7, lit("HIGH"))
            .when(col("epss_score") > 0.3, lit("MEDIUM"))
            .when(col("epss_score").isNotNull(), lit("LOW"))
            .otherwise(lit(None)))

        # ─── 5. UEH Priority Score ───────────────────────────────────
        # Formula: CVSS*8 + EPSS*20 + KEV*25 + exposure*5 (capped at 100)
        enriched_df = enriched_df \
            .withColumn("_cvss_component",
                        coalesce(col("cvss_base_score"), lit(5.0)) * 8) \
            .withColumn("_epss_component",
                        coalesce(col("epss_score"), lit(0.0)) * 20) \
            .withColumn("_kev_component",
                        when(col("is_in_kev") == True, lit(25.0)).otherwise(lit(0.0))) \
            .withColumn("_exposure_component",
                        when(col("total_affected_assets") > 100, lit(5.0))
                        .when(col("total_affected_assets") > 10, lit(3.0))
                        .when(col("total_affected_assets") > 0, lit(1.0))
                        .otherwise(lit(0.0))) \
            .withColumn("ueh_priority_score",
                        least(lit(100.0),
                              col("_cvss_component") + col("_epss_component") +
                              col("_kev_component") + col("_exposure_component")))

        # Priority tier
        enriched_df = enriched_df.withColumn("priority_tier",
            when(col("ueh_priority_score") >= 80, lit("P1_IMMEDIATE"))
            .when(col("ueh_priority_score") >= 60, lit("P2_URGENT"))
            .when(col("ueh_priority_score") >= 40, lit("P3_PLANNED"))
            .otherwise(lit("P4_MONITOR")))

        # ─── 6. Select final columns ─────────────────────────────────
        gold_df = enriched_df.select(
            "cve_id", "description", "severity", "cvss_base_score", "cvss_version",
            "published_date", "last_modified_date", "references_json",
            "affected_products_json", "weaknesses_json",
            "epss_score", "epss_percentile", "exploit_likelihood",
            "is_in_kev", "kev_date_added", "kev_due_date",
            "is_actively_exploited", "days_until_kev_deadline",
            "total_affected_assets", "critical_assets_affected",
            "production_assets_affected",
            "first_detected_in_org", "last_detected_in_org", "days_in_org",
            "ueh_priority_score", "priority_tier",
            "source_systems_json",
            current_timestamp().alias("computed_at"),
            lit(run_date).cast("date").alias("ingestion_date")
        )

        # ─── 7. Write to Gold ─────────────────────────────────────────
        record_count = gold_df.count()
        logger.info(f"Writing {record_count} enriched CVE records")

        gold_df.writeTo(gold_table).overwritePartitions()

        logger.info(f"SUCCESS: {record_count} CVEs written to Gold")

    except Exception as e:
        logger.error(f"FAILED: {e}")
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
