"""
=============================================================================
UEH Generic Bronze Loader
=============================================================================
ONE Spark job that loads Bronze for ANY adapter.

It does NOT contain adapter-specific logic. Instead it:
    1. Reads batch context from batch_registry (bronze_path, adapter_instance_id)
    2. Reads raw JSON chunks from HDFS
    3. Reads source_definition YAML to know array extraction path
    4. Explodes array → individual records → payload_json
    5. Writes to the correct Bronze Iceberg table
    6. Updates batch_registry → BRONZE_COMPLETE

Usage:
    spark-submit --conf ueh.environment=dev \
        generic_bronze_loader.py <batch_id>
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, col, lit, current_timestamp,
    input_file_name, monotonically_increasing_id,
    to_json, length, get_json_object
)
import sys
import traceback
import json


# ─── Configuration ────────────────────────────────────────────────────────────
DB_CONTROL = "t01_ueh_dev_ctl"
UEH_SCHEMA_VERSION = "brz_v1"


def main(batch_id: str):
    """Generic Bronze load for any adapter."""

    spark = SparkSession.builder \
        .appName(f"UEH_Bronze_Generic_{batch_id}") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()

    env = spark.conf.get("ueh.environment", "dev")
    db_control = f"t01_ueh_{env}_ctl"
    db_bronze = f"t01_ueh_{env}_brz"

    print(f"[UEH] Generic Bronze Loader: batch={batch_id}, env={env}")

    try:
        # ─── 1. Read batch context ──────────────────────────────────────
        batch = spark.sql(f"""
            SELECT b.batch_id, b.adapter_instance_id, b.batch_status,
                   b.bronze_path, b.ingestion_date, b.records_expected,
                   a.source_system, a.pagination_config_json
            FROM {db_control}.t01_ueh_ctl_batch_registry b
            JOIN {db_control}.t01_ueh_ctl_adapter_config a
              ON b.adapter_instance_id = a.adapter_instance_id
            WHERE b.batch_id = '{batch_id}'
        """).first()

        if batch is None:
            raise Exception(f"Batch '{batch_id}' not found!")
        if batch.batch_status != 'RAW_COMPLETE':
            raise Exception(f"Status is '{batch.batch_status}', expected RAW_COMPLETE")

        bronze_path = batch.bronze_path
        source_system = batch.source_system.lower()
        adapter_instance_id = batch.adapter_instance_id
        ingestion_date = str(batch.ingestion_date)

        # Determine target Bronze table
        bronze_table = f"{db_bronze}.t01_ueh_brz_{source_system}_raw"

        print(f"[UEH] Source: {source_system}, Path: {bronze_path}")
        print(f"[UEH] Target table: {bronze_table}")

        # ─── 2. Read raw chunks ─────────────────────────────────────────
        raw_df = spark.read.option("multiline", "true") \
            .json(f"{bronze_path}/chunk_*.json")

        # ─── 3. Determine array extraction path ─────────────────────────
        # Different APIs nest records differently:
        #   NVD: $.vulnerabilities[]
        #   EPSS: $.data[]
        #   Tenable: $ (each chunk IS the array)
        #   CISA KEV: $.vulnerabilities[]
        #
        # This is defined in source_definitions YAML or pagination_config
        # For now: detect common patterns
        array_field = _detect_array_field(raw_df, source_system)

        if array_field:
            exploded_df = raw_df.select(
                input_file_name().alias("_chunk_file"),
                explode(col(array_field)).alias("record")
            )
        else:
            # Flat structure — each row IS a record
            exploded_df = raw_df.select(
                input_file_name().alias("_chunk_file"),
                col("*").alias("record")  # Will need adjustment per source
            )

        # ─── 4. Build Bronze records ────────────────────────────────────
        # Extract source_record_id based on source system
        record_id_path = _get_record_id_path(source_system)

        bronze_df = exploded_df.select(
            lit(batch_id).alias("batch_id"),
            lit(adapter_instance_id).alias("adapter_instance_id"),
            current_timestamp().alias("ingestion_timestamp"),
            lit(ingestion_date).cast("date").alias("ingestion_date"),
            to_json(col("record")).alias("payload_json"),
            get_json_object(to_json(col("record")), record_id_path).alias("source_record_id"),
            col("_chunk_file").alias("chunk_file"),
            monotonically_increasing_id().cast("int").alias("record_index_in_chunk"),
            lit(f"{source_system}_{UEH_SCHEMA_VERSION}").alias("ueh_schema_version"),
            length(to_json(col("record"))).alias("dq_payload_size_bytes")
        )

        # ─── 5. Write to Bronze ─────────────────────────────────────────
        record_count = bronze_df.count()
        print(f"[UEH] Writing {record_count} records to {bronze_table}")
        bronze_df.writeTo(bronze_table).append()

        # ─── 6. Update batch_registry ───────────────────────────────────
        spark.sql(f"""
            UPDATE {db_control}.t01_ueh_ctl_batch_registry
            SET batch_status = 'BRONZE_COMPLETE',
                records_processed = {record_count},
                end_time = current_timestamp()
            WHERE batch_id = '{batch_id}'
        """)

        # Update adapter_state
        spark.sql(f"""
            UPDATE {db_control}.t01_ueh_ctl_adapter_state
            SET last_batch_id = '{batch_id}',
                updated_at = current_timestamp()
            WHERE adapter_instance_id = '{adapter_instance_id}'
        """)

        print(f"[UEH] SUCCESS: {record_count} records → BRONZE_COMPLETE")

    except Exception as e:
        error_msg = str(e).replace("'", "''")[:500]
        print(f"[UEH] ERROR: {e}")
        traceback.print_exc()
        try:
            spark.sql(f"""
                UPDATE {db_control}.t01_ueh_ctl_batch_registry
                SET batch_status = 'FAILED',
                    failure_reason = '{error_msg}',
                    failure_category = 'INTERNAL_ERROR',
                    end_time = current_timestamp()
                WHERE batch_id = '{batch_id}'
            """)
        except:
            pass
        raise
    finally:
        spark.stop()


def _detect_array_field(df, source_system: str) -> str:
    """Detect the array field in raw response based on source system."""
    known_arrays = {
        'nvd': 'vulnerabilities',
        'cisa_kev': 'vulnerabilities',
        'epss': 'data',
        'msrc': 'value',
    }
    if source_system in known_arrays:
        return known_arrays[source_system]

    # For unknown sources, check if common array fields exist
    fields = [f.name for f in df.schema.fields]
    for candidate in ['vulnerabilities', 'data', 'results', 'items', 'records']:
        if candidate in fields:
            return candidate
    return None


def _get_record_id_path(source_system: str) -> str:
    """Get JSONPath for source_record_id extraction."""
    known_ids = {
        'nvd': '$.cve.id',
        'epss': '$.cve',
        'cisa_kev': '$.cveID',
        'tenable': '$.asset.uuid',
        'sysdig': '$.id',
        'qualys': '$.id',
    }
    return known_ids.get(source_system, '$.id')


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: generic_bronze_loader.py <batch_id>")
        sys.exit(1)
    main(sys.argv[1])
