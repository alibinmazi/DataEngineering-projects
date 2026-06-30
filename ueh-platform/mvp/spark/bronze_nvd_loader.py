"""
=============================================================================
UEH MVP: Bronze Loader — NVD
=============================================================================
Database: t01_ueh_dev_ctl (control), t01_ueh_dev_brz (bronze)
Tables:
  - t01_ueh_dev_ctl.t01_ueh_ctl_batch_registry (read batch context)
  - t01_ueh_dev_brz.t01_ueh_brz_nvd_vulnerabilities (write Bronze records)

What it does:
    1. Reads batch context from batch_registry (bronze_path, batch_status)
    2. Reads raw JSON chunks from HDFS
    3. Explodes NVD response array into individual CVE records
    4. Writes to Bronze Iceberg table
    5. Updates batch_registry → BRONZE_COMPLETE

Usage:
    spark-submit --conf ueh.environment=dev \
        bronze_nvd_loader.py <batch_id>

Example:
    spark-submit --conf ueh.environment=dev \
        bronze_nvd_loader.py batch_20260608030000_nvd_prod_01
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, col, lit, current_timestamp,
    input_file_name, monotonically_increasing_id,
    to_json, length
)
import sys
import traceback


# ─── Configuration ────────────────────────────────────────────────────────────
# Database names following your naming convention
DB_CONTROL = "t01_ueh_dev_ctl"
DB_BRONZE = "t01_ueh_dev_brz"

# Table names
TBL_BATCH_REGISTRY = f"{DB_CONTROL}.t01_ueh_ctl_batch_registry"
TBL_ADAPTER_STATE = f"{DB_CONTROL}.t01_ueh_ctl_adapter_state"
TBL_BRONZE_NVD = f"{DB_BRONZE}.t01_ueh_brz_nvd_vulnerabilities"

# Schema version (update when Bronze parsing logic changes)
UEH_SCHEMA_VERSION = "nvd_brz_v1"


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

    print(f"[UEH] ═══════════════════════════════════════════════════════")
    print(f"[UEH] Bronze NVD Loader Starting")
    print(f"[UEH] Batch: {batch_id}")
    print(f"[UEH] Control DB: {DB_CONTROL}")
    print(f"[UEH] Bronze DB: {DB_BRONZE}")
    print(f"[UEH] ═══════════════════════════════════════════════════════")

    try:
        # ─── 2. Read batch context from batch_registry ───────────────────
        batch = spark.sql(f"""
            SELECT 
                org_id,
                batch_id,
                adapter_instance_id,
                batch_status,
                bronze_path,
                ingestion_date,
                load_type,
                records_expected
            FROM {TBL_BATCH_REGISTRY}
            WHERE batch_id = '{batch_id}'
        """).first()

        if batch is None:
            raise Exception(f"Batch '{batch_id}' NOT FOUND in {TBL_BATCH_REGISTRY}")

        if batch.batch_status != 'RAW_COMPLETE':
            raise Exception(
                f"Batch status is '{batch.batch_status}', expected 'RAW_COMPLETE'. "
                f"Cannot proceed with Bronze load."
            )

        bronze_path = batch.bronze_path
        adapter_instance_id = batch.adapter_instance_id
        org_id = batch.org_id
        ingestion_date = str(batch.ingestion_date)

        print(f"[UEH] Batch found: status={batch.batch_status}")
        print(f"[UEH] Reading raw files from: {bronze_path}")
        print(f"[UEH] Expected records: {batch.records_expected}")

        # ─── 3. Read raw JSON chunk files from HDFS ──────────────────────
        raw_df = spark.read \
            .option("multiline", "true") \
            .json(f"{bronze_path}/chunk_*.json")

        print(f"[UEH] Raw chunk files read successfully")

        # ─── 4. Explode NVD vulnerabilities array ────────────────────────
        # NVD response structure:
        #   {"totalResults": N, "vulnerabilities": [{...}, {...}, ...]}
        # Each element in "vulnerabilities" = one CVE record
        exploded_df = raw_df.select(
            input_file_name().alias("_chunk_file"),
            explode(col("vulnerabilities")).alias("vuln_record")
        )

        # ─── 5. Build Bronze records ─────────────────────────────────────
        bronze_df = exploded_df.select(
            # Batch linkage
            lit(batch_id).alias("batch_id"),
            lit(adapter_instance_id).alias("adapter_instance_id"),
            current_timestamp().alias("ingestion_timestamp"),
            lit(ingestion_date).cast("date").alias("ingestion_date"),

            # Raw payload — complete record as JSON string
            to_json(col("vuln_record")).alias("payload_json"),

            # Operational reference (NOT business logic)
            col("vuln_record.cve.id").alias("source_record_id"),
            col("_chunk_file").alias("chunk_file"),
            monotonically_increasing_id().cast("int").alias("record_index_in_chunk"),

            # Schema version — for replay compatibility
            lit(UEH_SCHEMA_VERSION).alias("ueh_schema_version"),

            # Lightweight DQ
            length(to_json(col("vuln_record"))).alias("dq_payload_size_bytes")
        )

        # ─── 6. Write to Bronze Iceberg table ────────────────────────────
        record_count = bronze_df.count()
        print(f"[UEH] Writing {record_count} records to {TBL_BRONZE_NVD}")

        bronze_df.writeTo(TBL_BRONZE_NVD).append()

        print(f"[UEH] Write complete.")

        # ─── 7. Update batch_registry → BRONZE_COMPLETE ──────────────────
        spark.sql(f"""
            UPDATE {TBL_BATCH_REGISTRY}
            SET batch_status = 'BRONZE_COMPLETE',
                records_processed = {record_count},
                end_time = current_timestamp()
            WHERE batch_id = '{batch_id}'
        """)

        # ─── 8. Update adapter_state with last_batch_id ──────────────────
        spark.sql(f"""
            UPDATE {TBL_ADAPTER_STATE}
            SET last_batch_id = '{batch_id}',
                updated_at = current_timestamp()
            WHERE adapter_instance_id = '{adapter_instance_id}'
              AND org_id = '{org_id}'
        """)

        print(f"[UEH] ═══════════════════════════════════════════════════════")
        print(f"[UEH] SUCCESS: {record_count} records → BRONZE_COMPLETE")
        print(f"[UEH] ═══════════════════════════════════════════════════════")

    except Exception as e:
        error_msg = str(e).replace("'", "''")[:500]
        print(f"[UEH] ERROR: {e}")
        traceback.print_exc()

        # Update batch_registry → FAILED
        try:
            spark.sql(f"""
                UPDATE {TBL_BATCH_REGISTRY}
                SET batch_status = 'FAILED',
                    failure_reason = '{error_msg}',
                    failure_category = 'INTERNAL_ERROR',
                    end_time = current_timestamp()
                WHERE batch_id = '{batch_id}'
            """)
            print(f"[UEH] Batch status updated to FAILED in registry.")
        except Exception as inner_e:
            print(f"[UEH] CRITICAL: Could not update failure status: {inner_e}")

        raise

    finally:
        spark.stop()


# ─── Entry Point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("=" * 60)
        print("UEH Bronze NVD Loader")
        print("=" * 60)
        print("Usage: bronze_nvd_loader.py <batch_id>")
        print("Example: bronze_nvd_loader.py batch_20260608030000_nvd_prod_01")
        print("")
        print("The batch must exist in t01_ueh_ctl_batch_registry")
        print("with batch_status = 'RAW_COMPLETE'")
        sys.exit(1)

    main(sys.argv[1])
