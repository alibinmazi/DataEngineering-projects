"""
=============================================================================
UEH Gold: Exposure Summary
=============================================================================
Target table: t01_ueh_gld_exposure_summary
Sources: JOIN of slv_vulnerability_findings + slv_vulnerability_intel + slv_assets
Strategy: OVERWRITE PARTITION (rebuild daily from Silver)

What it does:
    1. Read all OPEN findings from slv_vulnerability_findings
    2. LEFT JOIN to slv_vulnerability_intel (enrich with CVSS, EPSS, KEV)
    3. LEFT JOIN to slv_assets (enrich with asset criticality, owner, env)
    4. Compute UEH risk_score and priority_rank
    5. Compute days_exposed, sla_status
    6. OVERWRITE today's partition in Gold table

Usage:
    spark-submit --conf ueh.environment=dev \
        gold_exposure_summary.py --run_date 2026-06-20
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, datediff,
    when, coalesce, row_number, greatest
)
from pyspark.sql.window import Window
import argparse
import logging
import traceback

logging.basicConfig(level=logging.INFO, format='[UEH-Gold-Exposure] %(levelname)s: %(message)s')
logger = logging.getLogger("UEH-Gold-Exposure")


def main():
    parser = argparse.ArgumentParser(description="UEH Gold: Exposure Summary")
    parser.add_argument("--run_date", required=False, help="Date to compute (YYYY-MM-DD). Default: today")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("UEH_Gold_ExposureSummary") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

    env = spark.conf.get("ueh.environment", "dev")
    db_silver = f"t01_ueh_{env}_slv"
    db_gold = f"t01_ueh_{env}_gld"
    gold_table = f"{db_gold}.t01_ueh_gld_exposure_summary"

    run_date = args.run_date or str(spark.sql("SELECT current_date()").first()[0])

    logger.info("=" * 60)
    logger.info(f"Gold Exposure Summary: run_date={run_date}, env={env}")
    logger.info("=" * 60)

    try:
        # ─── 1. Read Silver: Vulnerability Findings (latest per finding) ─
        findings_df = spark.sql(f"""
            SELECT
                finding_id,
                source_system,
                source_finding_id,
                cve_id,
                vulnerability_name,
                asset_id,
                source_asset_id,
                asset_ip,
                asset_hostname,
                asset_fqdn,
                asset_os,
                severity,
                cvss_base_score AS finding_cvss,
                source_risk_score,
                status,
                first_seen,
                last_seen,
                fixed_at,
                solution,
                output,
                adapter_instance_id,
                batch_id
            FROM {db_silver}.t01_ueh_slv_vulnerability_findings
            WHERE status IN ('OPEN', 'REOPENED')
        """)

        findings_count = findings_df.count()
        logger.info(f"Open findings from Silver: {findings_count}")

        if findings_count == 0:
            logger.info("No open findings. Writing empty Gold partition.")
            spark.sql(f"""
                DELETE FROM {gold_table} WHERE ingestion_date = DATE '{run_date}'
            """)
            return

        # ─── 2. Read Silver: Vulnerability Intelligence ──────────────────
        intel_df = spark.sql(f"""
            SELECT
                cve_id AS intel_cve_id,
                cvss_base_score AS intel_cvss,
                cvss_version,
                severity AS intel_severity,
                description,
                epss_score,
                epss_percentile,
                is_in_kev,
                kev_due_date,
                is_actively_exploited,
                published_date,
                last_modified_date
            FROM {db_silver}.t01_ueh_slv_vulnerability_intel
        """)

        logger.info(f"Vulnerability intel records: {intel_df.count()}")

        # ─── 3. Read Silver: Assets ──────────────────────────────────────
        assets_df = spark.sql(f"""
            SELECT
                asset_id AS asset_lookup_id,
                asset_type,
                criticality AS asset_criticality,
                environment AS asset_environment,
                business_unit AS asset_business_unit,
                owner AS asset_owner
            FROM {db_silver}.t01_ueh_slv_assets
            WHERE is_active = TRUE
        """)

        logger.info(f"Active assets: {assets_df.count()}")

        # ─── 4. JOIN: Findings + Intel + Assets ──────────────────────────
        enriched_df = findings_df \
            .join(intel_df, findings_df.cve_id == intel_df.intel_cve_id, "left") \
            .join(assets_df, findings_df.asset_id == assets_df.asset_lookup_id, "left") \
            .drop("intel_cve_id", "asset_lookup_id")

        # ─── 5. Compute Risk Score ──────────────────────────────────────
        #
        # UEH Risk Score (0-100) formula:
        #   Base: CVSS (0-10) * 10 = 0-100
        #   Boost: EPSS > 0.7 → +15
        #   Boost: In KEV → +20
        #   Boost: Asset criticality CRITICAL → +10
        #   Boost: Production environment → +5
        #   Cap at 100
        #
        enriched_df = enriched_df \
            .withColumn("cvss_used",
                        coalesce(col("intel_cvss"), col("finding_cvss"), lit(5.0))) \
            .withColumn("_base_score", col("cvss_used") * 10) \
            .withColumn("_epss_boost",
                        when(col("epss_score") > 0.7, lit(15.0)).otherwise(lit(0.0))) \
            .withColumn("_kev_boost",
                        when(col("is_in_kev") == True, lit(20.0)).otherwise(lit(0.0))) \
            .withColumn("_asset_boost",
                        when(col("asset_criticality") == "CRITICAL", lit(10.0)).otherwise(lit(0.0))) \
            .withColumn("_env_boost",
                        when(col("asset_environment") == "PRODUCTION", lit(5.0)).otherwise(lit(0.0))) \
            .withColumn("risk_score",
                        greatest(lit(0.0),
                                 least(lit(100.0),
                                       col("_base_score") + col("_epss_boost") +
                                       col("_kev_boost") + col("_asset_boost") + col("_env_boost"))))

        # Risk category
        enriched_df = enriched_df.withColumn("risk_category",
            when(col("risk_score") >= 80, lit("CRITICAL_RISK"))
            .when(col("risk_score") >= 60, lit("HIGH_RISK"))
            .when(col("risk_score") >= 40, lit("MEDIUM_RISK"))
            .otherwise(lit("LOW_RISK"))
        )

        # ─── 6. Compute Timing Metrics ───────────────────────────────────
        enriched_df = enriched_df \
            .withColumn("days_exposed",
                        datediff(current_date(), col("first_seen")).cast("int")) \
            .withColumn("days_to_remediate",
                        when(col("fixed_at").isNotNull(),
                             datediff(col("fixed_at"), col("first_seen")).cast("int"))
                        .otherwise(lit(None))) \
            .withColumn("sla_status",
                        when(col("days_exposed") > 90, lit("SLA_BREACHED"))
                        .when(col("days_exposed") > 60, lit("APPROACHING_SLA"))
                        .otherwise(lit("WITHIN_SLA")))

        # ─── 7. Compute Priority Rank ────────────────────────────────────
        window = Window.orderBy(col("risk_score").desc())
        enriched_df = enriched_df.withColumn("priority_rank",
                                             row_number().over(window))

        # Use CVSS from intel (enriched) for final column
        enriched_df = enriched_df \
            .withColumn("cvss_base_score", col("cvss_used")) \
            .withColumn("severity",
                        coalesce(col("intel_severity"), col("severity")))

        # ─── 8. Select Final Columns ─────────────────────────────────────
        gold_df = enriched_df.select(
            "finding_id", "source_system", "cve_id", "vulnerability_name",
            "description", "severity", "cvss_base_score", "cvss_version",
            "epss_score", "epss_percentile", "is_in_kev", "kev_due_date",
            "is_actively_exploited",
            "asset_id", "asset_ip", "asset_hostname", "asset_fqdn", "asset_os",
            "asset_type", "asset_criticality", "asset_environment",
            "asset_business_unit", "asset_owner",
            "risk_score", "risk_category", "priority_rank",
            "first_seen", "last_seen", "days_exposed", "sla_status",
            "status", "fixed_at", "days_to_remediate",
            "source_risk_score", "solution", "output",
            "adapter_instance_id", "batch_id",
            current_timestamp().alias("gold_computed_at"),
            lit(run_date).cast("date").alias("ingestion_date")
        )

        # ─── 9. Write to Gold (OVERWRITE today's partition) ───────────────
        record_count = gold_df.count()
        logger.info(f"Writing {record_count} records to {gold_table}")

        gold_df.writeTo(gold_table).overwritePartitions()

        logger.info("=" * 60)
        logger.info(f"SUCCESS: {record_count} exposure records written to Gold")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"FAILED: {e}")
        traceback.print_exc()
        raise
    finally:
        spark.stop()


# Need pyspark.sql.functions.least for risk score cap
from pyspark.sql.functions import least

if __name__ == "__main__":
    main()
