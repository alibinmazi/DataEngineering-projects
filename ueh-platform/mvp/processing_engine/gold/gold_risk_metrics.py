"""
=============================================================================
UEH Gold: Risk Metrics (Aggregated)
=============================================================================
Target table: t01_ueh_gld_risk_metrics
Source: t01_ueh_gld_exposure_summary (reads from Gold exposure table)
Strategy: OVERWRITE partition daily

Computes aggregated metrics by multiple dimensions:
    - OVERALL (total org)
    - BY_SEVERITY (per severity level)
    - BY_BUSINESS_UNIT (per BU)
    - BY_ASSET_TYPE (per asset category)
    - BY_ENVIRONMENT (per environment)
    - BY_SOURCE (per scanner)

Usage:
    spark-submit --conf ueh.environment=dev \
        gold_risk_metrics.py --run_date 2026-06-20
=============================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, count, sum as spark_sum,
    avg, max as spark_max, min as spark_min, countDistinct, when
)
import argparse
import logging
import traceback

logging.basicConfig(level=logging.INFO, format='[UEH-Gold-Metrics] %(levelname)s: %(message)s')
logger = logging.getLogger("UEH-Gold-Metrics")


def main():
    parser = argparse.ArgumentParser(description="UEH Gold: Risk Metrics")
    parser.add_argument("--run_date", required=False)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("UEH_Gold_RiskMetrics") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

    env = spark.conf.get("ueh.environment", "dev")
    db_gold = f"t01_ueh_{env}_gld"
    exposure_table = f"{db_gold}.t01_ueh_gld_exposure_summary"
    metrics_table = f"{db_gold}.t01_ueh_gld_risk_metrics"

    run_date = args.run_date or str(spark.sql("SELECT current_date()").first()[0])

    logger.info("=" * 60)
    logger.info(f"Gold Risk Metrics: run_date={run_date}")
    logger.info("=" * 60)

    try:
        # Read today's exposure summary
        exposure_df = spark.table(exposure_table) \
            .where(f"ingestion_date = DATE '{run_date}'")

        exposure_count = exposure_df.count()
        logger.info(f"Exposure records for {run_date}: {exposure_count}")

        if exposure_count == 0:
            logger.warning("No exposure data for today. Skipping metrics.")
            return

        # Compute metrics for each dimension
        metrics_frames = []

        # OVERALL
        metrics_frames.append(
            _compute_metrics(exposure_df, "OVERALL", "ALL", run_date)
        )

        # BY_SEVERITY
        for sev in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFORMATIONAL']:
            filtered = exposure_df.where(f"severity = '{sev}'")
            if filtered.count() > 0:
                metrics_frames.append(
                    _compute_metrics(filtered, "BY_SEVERITY", sev, run_date)
                )

        # BY_ENVIRONMENT
        envs = [row.asset_environment for row in
                exposure_df.select("asset_environment").distinct().collect()
                if row.asset_environment]
        for env_val in envs:
            filtered = exposure_df.where(f"asset_environment = '{env_val}'")
            metrics_frames.append(
                _compute_metrics(filtered, "BY_ENVIRONMENT", env_val, run_date)
            )

        # BY_BUSINESS_UNIT
        bus = [row.asset_business_unit for row in
               exposure_df.select("asset_business_unit").distinct().collect()
               if row.asset_business_unit]
        for bu in bus:
            filtered = exposure_df.where(f"asset_business_unit = '{bu}'")
            metrics_frames.append(
                _compute_metrics(filtered, "BY_BUSINESS_UNIT", bu, run_date)
            )

        # BY_SOURCE
        sources = [row.source_system for row in
                   exposure_df.select("source_system").distinct().collect()
                   if row.source_system]
        for src in sources:
            filtered = exposure_df.where(f"source_system = '{src}'")
            metrics_frames.append(
                _compute_metrics(filtered, "BY_SOURCE", src, run_date)
            )

        # Union all metrics
        from functools import reduce
        all_metrics = reduce(DataFrame.unionAll, metrics_frames)

        # Write to Gold metrics table
        record_count = all_metrics.count()
        logger.info(f"Writing {record_count} metric rows to {metrics_table}")

        all_metrics.writeTo(metrics_table).overwritePartitions()

        logger.info(f"SUCCESS: {record_count} metric rows written")

    except Exception as e:
        logger.error(f"FAILED: {e}")
        traceback.print_exc()
        raise
    finally:
        spark.stop()


def _compute_metrics(df: DataFrame, dim_type: str, dim_value: str, run_date: str) -> DataFrame:
    """Compute aggregated metrics for a filtered DataFrame."""
    spark = df.sparkSession

    result = df.agg(
        count("*").alias("total_findings"),
        spark_sum(when(col("status") == "OPEN", 1).otherwise(0)).alias("open_findings"),
        spark_sum(when(col("status") == "FIXED", 1).otherwise(0)).alias("fixed_findings"),
        spark_sum(when((col("severity") == "CRITICAL") & (col("status") == "OPEN"), 1).otherwise(0)).alias("critical_open"),
        spark_sum(when((col("severity") == "HIGH") & (col("status") == "OPEN"), 1).otherwise(0)).alias("high_open"),
        spark_sum(when(col("is_in_kev") == True, 1).otherwise(0)).alias("kev_open"),
        spark_sum(when(col("epss_score") > 0.7, 1).otherwise(0)).alias("exploitable_open"),
        avg("risk_score").alias("avg_risk_score"),
        spark_max("risk_score").alias("max_risk_score"),
        avg("cvss_base_score").alias("avg_cvss"),
        avg("epss_score").alias("avg_epss"),
        avg("days_exposed").alias("avg_days_exposed"),
        spark_max("days_exposed").alias("max_days_exposed"),
        avg("days_to_remediate").alias("avg_days_to_remediate"),
        spark_sum(when(col("sla_status") == "SLA_BREACHED", 1).otherwise(0)).alias("sla_breach_count"),
        countDistinct("asset_id").alias("unique_assets_affected"),
        spark_sum(when(col("asset_criticality") == "CRITICAL", 1).otherwise(0)).alias("critical_assets_affected"),
    )

    # Add dimension columns
    result = result \
        .withColumn("metric_date", lit(run_date).cast("date")) \
        .withColumn("dimension_type", lit(dim_type)) \
        .withColumn("dimension_value", lit(dim_value)) \
        .withColumn("new_findings_today", lit(0)) \
        .withColumn("fixed_today", lit(0)) \
        .withColumn("reopened_today", lit(0)) \
        .withColumn("net_change", lit(0)) \
        .withColumn("computed_at", current_timestamp())

    return result


if __name__ == "__main__":
    main()
