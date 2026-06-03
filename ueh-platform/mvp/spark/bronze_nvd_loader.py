"""
=============================================================================
UEH MVP: Bronze Loader — NVD
=============================================================================
Single self-contained Spark job. No base class needed for MVP.

What it does:
    1. Reads batch context from batch_registry (where are the raw files?)
    2. Reads raw JSON chunks from HDFS
    3. Explodes NVD response array into individual CVE records
    4. Writes to Bronze Iceberg table
    5. Updates batch_registry → BRONZE_COMPLETE

Usage:
    spark-submit --conf ueh.environment=dev \
        mvp/spark/bronze_nvd_loader.py <batch_id>

Example:
    spark-submit --conf ueh.environment=dev \
        mvp/spark/bronze_nvd_loader.py batch_20260603030000_nvd_public_01
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, col, lit, current_timestamp,
    input_file_name, monotonically_increasing_id,
    to_json, length
)
import sys


def main(batch_id: str):
    """Load a single RAW_COMPLETE batch into Bronze Iceberg table."""

    # ─── 1. Initialize Spark ─────────────────────────────────────────────
    spark = SparkSession.builder \
        .appName(f"UEH_Bronze_NVD_{batch_id}") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

    env = spark.conf.get("ueh.environment", "dev")
    db_control = f"ueh_{env}_control"
    db_bronze = f"ueh_{env}_bronze"

    print(f"[UEH] Starting Bronze load: batch={batch_id}, env={env}")

    # ─── 2. Read batch context from control table ────────────────────────
    batch = spark.sql(f"""
        SELECT batch_id, adapter_instance_id, ingestion_date, 
               load_type, bronze_path, status
        FROM {db_control}.t01_ueh_ctl_batch_registry
        WHERE batch_id = '{batch_id}'
    """).first()

    if batch is None:
        raise Exception(f"Batch '{batch_id}' not found in batch_registry!")

    if batch.status != 'RAW_COMPLETE':
        raise Exception(f"Batch status is '{batch.status}', expected 'RAW_COMPLETE'")

    bronze_path = batch.bronze_path
    adapter_instance_id = batch.adapter_instance_id
    ingestion_date = str(batch.ingestion_date)

    print(f"[UEH] Reading raw files from: {bronze_path}")

    # ─── 3. Read raw JSON chunk files ────────────────────────────────────
    raw_df = spark.read \
        .option("multiline", "true") \
        .json(f"{bronze_path}/chunk_*.json")

    # ─── 4. Explode NVD vulnerabilities array ────────────────────────────
    # NVD response: {"vulnerabilities": [{...}, {...}, ...], "totalResults": N}
    # Each element in the array = one CVE record
    exploded_df = raw_df.select(
        input_file_name().alias("_chunk_file"),
        explode(col("vulnerabilities")).alias("vuln_record")
    )

    # ─── 5. Build Bronze records ─────────────────────────────────────────
    bronze_df = exploded_df.select(
        # Batch linkage
        lit(batch_id).alias("batch_id"),
        lit(adapter_instance_id).alias("adapter_instance_id"),
        current_timestamp().alias("ingestion_timestamp"),
        lit(ingestion_date).cast("date").alias("ingestion_date"),

        # Raw payload — complete record as JSON string
        to_json(col("vuln_record")).alias("payload_json"),

        # Operational reference
        col("vuln_record.cve.id").alias("source_record_id"),
        col("_chunk_file").alias("chunk_file"),
        monotonically_increasing_id().cast("int").alias("record_index_in_chunk"),

        # Schema version
        lit("nvd_brz_v1").alias("ueh_schema_version"),

        # DQ
        length(to_json(col("vuln_record"))).alias("dq_payload_size_bytes")
    )

    # ─── 6. Write to Bronze Iceberg table ────────────────────────────────
    target_table = f"{db_bronze}.t01_ueh_brz_nvd_vulnerabilities"
    record_count = bronze_df.count()

    print(f"[UEH] Writing {record_count} records to {target_table}")

    bronze_df.writeTo(target_table).append()

    # ─── 7. Update batch_registry → BRONZE_COMPLETE ──────────────────────
    spark.sql(f"""
        UPDATE {db_control}.t01_ueh_ctl_batch_registry
        SET status = 'BRONZE_COMPLETE',
            records_ingested = {record_count},
            completed_at = current_timestamp(),
            updated_at = current_timestamp()
        WHERE batch_id = '{batch_id}'
    """)

    print(f"[UEH] DONE! {record_count} records loaded. Status → BRONZE_COMPLETE")
    spark.stop()


# ─── Entry Point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: bronze_nvd_loader.py <batch_id>")
        print("Example: bronze_nvd_loader.py batch_20260603030000_nvd_public_01")
        sys.exit(1)

    main(sys.argv[1])
