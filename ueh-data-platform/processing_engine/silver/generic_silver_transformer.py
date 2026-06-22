"""
=============================================================================
UEH Generic Silver Transformer
=============================================================================
ONE Spark job that transforms Bronze → Silver for ANY adapter.

It does NOT contain adapter-specific logic. Instead it:
    1. Reads batch context from batch_registry (which adapter, which batch)
    2. Reads field_mapping rules from control table (analyst-configured)
    3. Reads Bronze records for the batch (payload_json)
    4. Applies mappings dynamically: extract → transform → validate
    5. Writes to the correct Silver Iceberg table
    6. Updates batch_registry → SILVER_COMPLETE

Supports transformation types:
    DIRECT     → Use raw extracted value as-is
    CAST       → Cast to type (DOUBLE, TIMESTAMP, INT, DATE)
    UPPER      → Uppercase (for enum standardization)
    LOWER      → Lowercase
    TRIM       → Trim whitespace
    TO_JSON    → Keep nested object/array as JSON string
    LOOKUP     → Map value using lookup dictionary
    EXPRESSION → Spark SQL expression

Usage:
    spark-submit --conf ueh.environment=dev \
        generic_silver_transformer.py --batch_id <batch_id>

Example:
    spark-submit --conf ueh.environment=dev \
        generic_silver_transformer.py --batch_id batch_20260608030000_nvd_prod_01
=============================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, get_json_object,
    upper, lower, trim, length, when, coalesce,
    to_timestamp, md5, concat_ws, expr
)
from pyspark.sql.types import DoubleType, IntegerType, DateType, TimestampType
import argparse
import logging
import traceback
import json

# ─── Setup ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='[UEH-Silver] %(levelname)s: %(message)s')
logger = logging.getLogger("UEH-Silver")


# ─── Source-to-Silver Table Routing ───────────────────────────────────────────
# Maps source_system category → target Silver table
# This determines which Silver table a source writes into

SOURCE_TO_SILVER_TABLE = {
    # Vulnerability Intelligence → slv_vulnerability_intel
    'NVD': 'vulnerability_intel',
    'EPSS': 'vulnerability_intel',
    'CISA_KEV': 'vulnerability_intel',
    'MSRC': 'vulnerability_intel',

    # Vulnerability Scanners → slv_vulnerability_findings
    'TENABLE': 'vulnerability_findings',
    'SYSDIG': 'vulnerability_findings',
    'QUALYS': 'vulnerability_findings',
    'FORTIFY': 'vulnerability_findings',

    # Asset Inventory → slv_assets
    'BMC_ADDM': 'assets',
    'CMDB': 'assets',
}

# Silver table write strategy
SILVER_WRITE_STRATEGY = {
    'vulnerability_intel': 'MERGE',      # Upsert on cve_id
    'vulnerability_findings': 'APPEND',  # Point-in-time snapshots
    'assets': 'MERGE',                   # Upsert on asset_id
}

# Merge key per Silver table (used for MERGE strategy)
SILVER_MERGE_KEYS = {
    'vulnerability_intel': 'cve_id',
    'assets': 'asset_id',
}


def main():
    # ─── Parse Arguments ──────────────────────────────────────────────────
    parser = argparse.ArgumentParser(description="UEH Generic Silver Transformer")
    parser.add_argument("--batch_id", required=True, help="Batch ID to process")
    args = parser.parse_args()
    batch_id = args.batch_id

    # ─── Initialize Spark ─────────────────────────────────────────────────
    spark = SparkSession.builder \
        .appName(f"UEH_Silver_{batch_id}") \
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
    logger.info("UEH Generic Silver Transformer")
    logger.info(f"Batch: {batch_id}")
    logger.info(f"Environment: {env}")
    logger.info("=" * 60)

    try:
        # ─── 1. Read Batch Context ───────────────────────────────────────
        batch = spark.sql(f"""
            SELECT b.batch_id, b.adapter_instance_id, b.batch_status,
                   b.ingestion_date, b.records_processed,
                   a.source_system, a.org_id
            FROM {db_control}.t01_ueh_ctl_batch_registry b
            JOIN {db_control}.t01_ueh_ctl_adapter_config a
              ON b.adapter_instance_id = a.adapter_instance_id
            WHERE b.batch_id = '{batch_id}'
        """).first()

        if batch is None:
            raise Exception(f"Batch '{batch_id}' not found in batch_registry!")

        if batch.batch_status != 'BRONZE_COMPLETE':
            raise Exception(
                f"Batch status is '{batch.batch_status}', "
                f"expected 'BRONZE_COMPLETE'. Cannot run Silver."
            )

        source_system = batch.source_system
        adapter_instance_id = batch.adapter_instance_id
        org_id = batch.org_id
        ingestion_date = str(batch.ingestion_date)

        logger.info(f"Source system: {source_system}")
        logger.info(f"Adapter: {adapter_instance_id}")
        logger.info(f"Ingestion date: {ingestion_date}")

        # ─── 2. Determine Target Silver Table ────────────────────────────
        silver_domain = SOURCE_TO_SILVER_TABLE.get(source_system)
        if silver_domain is None:
            raise Exception(
                f"No Silver table mapping for source_system='{source_system}'. "
                f"Add it to SOURCE_TO_SILVER_TABLE dict."
            )

        silver_table = f"{db_silver}.t01_ueh_slv_{silver_domain}"
        write_strategy = SILVER_WRITE_STRATEGY.get(silver_domain, 'APPEND')

        logger.info(f"Target Silver table: {silver_table}")
        logger.info(f"Write strategy: {write_strategy}")

        # ─── 3. Read Field Mappings ──────────────────────────────────────
        mappings_df = spark.sql(f"""
            SELECT mapping_id, source_json_path, target_field,
                   transformation_type, transformation_config, is_required
            FROM {db_control}.t01_ueh_ctl_field_mapping
            WHERE source_system = '{source_system}'
              AND org_id = '{org_id}'
              AND is_active = TRUE
            ORDER BY mapping_id
        """)

        mappings = mappings_df.collect()

        if len(mappings) == 0:
            raise Exception(
                f"No active field mappings found for source_system='{source_system}', "
                f"org_id='{org_id}'. Analyst must configure mappings in UEH Dashboard."
            )

        logger.info(f"Loaded {len(mappings)} field mappings")

        # ─── 4. Read Bronze Records for This Batch ───────────────────────
        bronze_table = f"{db_bronze}.t01_ueh_brz_{source_system.lower()}_raw"

        bronze_df = spark.sql(f"""
            SELECT batch_id, adapter_instance_id, ingestion_date,
                   payload_json, source_record_id
            FROM {bronze_table}
            WHERE batch_id = '{batch_id}'
        """)

        bronze_count = bronze_df.count()
        logger.info(f"Bronze records for batch: {bronze_count}")

        if bronze_count == 0:
            logger.warning("No Bronze records found for this batch. Marking SILVER_COMPLETE with 0 records.")
            _update_batch_status(spark, db_control, batch_id, 'SILVER_COMPLETE', 0)
            return

        # ─── 5. Apply Field Mappings (NORMALIZE) ─────────────────────────
        silver_df = _apply_mappings(bronze_df, mappings)

        # ─── 6. Add UEH Metadata Columns ─────────────────────────────────
        silver_df = silver_df \
            .withColumn("adapter_instance_id", lit(adapter_instance_id)) \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("ingestion_date", lit(ingestion_date).cast("date"))

        # Add source_systems_json for vulnerability_intel
        if silver_domain == 'vulnerability_intel':
            silver_df = silver_df \
                .withColumn("source_systems_json", lit(f'["{source_system}"]')) \
                .withColumn("nvd_batch_id",
                            when(lit(source_system) == "NVD", lit(batch_id)).otherwise(lit(None))) \
                .withColumn("epss_batch_id",
                            when(lit(source_system) == "EPSS", lit(batch_id)).otherwise(lit(None))) \
                .withColumn("cisa_batch_id",
                            when(lit(source_system) == "CISA_KEV", lit(batch_id)).otherwise(lit(None))) \
                .withColumn("first_seen_in_ueh", current_timestamp()) \
                .withColumn("last_updated_in_ueh", current_timestamp()) \
                .withColumn("is_in_kev", lit(source_system == "CISA_KEV")) \
                .withColumn("is_actively_exploited", lit(False))

        # Add finding_id for vulnerability_findings
        if silver_domain == 'vulnerability_findings':
            silver_df = silver_df \
                .withColumn("source_system", lit(source_system)) \
                .withColumn("finding_id",
                            md5(concat_ws("||",
                                          lit(source_system),
                                          col("source_finding_id"),
                                          coalesce(col("source_asset_id"), lit("unknown"))
                                          )))

        # Add asset_id for assets
        if silver_domain == 'assets':
            silver_df = silver_df \
                .withColumn("source_system", lit(source_system)) \
                .withColumn("asset_id",
                            md5(concat_ws("||",
                                          lit(source_system),
                                          coalesce(col("source_asset_id"), lit("unknown"))
                                          ))) \
                .withColumn("source_systems_json", lit(f'["{source_system}"]')) \
                .withColumn("last_source_batch_id", lit(batch_id)) \
                .withColumn("is_active", lit(True))

        # ─── 7. CLEAN ────────────────────────────────────────────────────
        silver_df = _clean_data(silver_df, mappings)

        # ─── 8. VALIDATE (Add DQ Flags) ──────────────────────────────────
        silver_df = _validate_data(silver_df, silver_domain)

        # ─── 9. Write to Silver Table ─────────────────────────────────────
        record_count = silver_df.count()
        logger.info(f"Writing {record_count} records to {silver_table} (strategy={write_strategy})")

        if write_strategy == 'MERGE' and silver_domain in SILVER_MERGE_KEYS:
            merge_key = SILVER_MERGE_KEYS[silver_domain]
            _write_merge(spark, silver_df, silver_table, merge_key, db_silver)
        else:
            silver_df.writeTo(silver_table).append()

        logger.info(f"Write complete.")

        # ─── 10. Update batch_registry → SILVER_COMPLETE ─────────────────
        _update_batch_status(spark, db_control, batch_id, 'SILVER_COMPLETE', record_count)

        logger.info("=" * 60)
        logger.info(f"SUCCESS: {record_count} records → SILVER_COMPLETE")
        logger.info("=" * 60)

    except Exception as e:
        error_msg = str(e).replace("'", "''")[:500]
        logger.error(f"FAILED: {e}")
        traceback.print_exc()

        try:
            _update_batch_status(spark, db_control, batch_id, 'FAILED', 0, error_msg)
        except Exception as inner_e:
            logger.error(f"Could not update failure status: {inner_e}")

        raise

    finally:
        spark.stop()


# =============================================================================
# MAPPING ENGINE
# =============================================================================

def _apply_mappings(bronze_df: DataFrame, mappings: list) -> DataFrame:
    """
    Apply field mappings to extract values from payload_json.
    
    For each mapping rule:
      1. Extract value using get_json_object(payload_json, source_json_path)
      2. Apply transformation (CAST, UPPER, etc.)
      3. Alias as target_field
    """
    result_df = bronze_df

    for mapping in mappings:
        source_path = mapping.source_json_path
        target_field = mapping.target_field
        transform_type = (mapping.transformation_type or 'DIRECT').upper()
        transform_config = mapping.transformation_config

        # Extract raw value from JSON
        extracted_col = get_json_object(col("payload_json"), source_path)

        # Apply transformation
        transformed_col = _apply_transformation(
            extracted_col, transform_type, transform_config
        )

        # Add as new column
        result_df = result_df.withColumn(target_field, transformed_col)

    return result_df


def _apply_transformation(column, transform_type: str, config_json: str):
    """Apply a single transformation to an extracted column."""

    config = {}
    if config_json:
        try:
            config = json.loads(config_json)
        except json.JSONDecodeError:
            pass

    if transform_type == 'DIRECT':
        return column

    elif transform_type == 'CAST':
        cast_to = config.get('cast_to', 'STRING').upper()
        if cast_to == 'DOUBLE':
            return column.cast(DoubleType())
        elif cast_to == 'INT' or cast_to == 'INTEGER':
            return column.cast(IntegerType())
        elif cast_to == 'TIMESTAMP':
            fmt = config.get('format')
            if fmt:
                return to_timestamp(column, fmt)
            else:
                return to_timestamp(column)
        elif cast_to == 'DATE':
            return column.cast(DateType())
        else:
            return column

    elif transform_type == 'UPPER':
        return upper(column)

    elif transform_type == 'LOWER':
        return lower(column)

    elif transform_type == 'TRIM':
        return trim(column)

    elif transform_type == 'TO_JSON':
        # Value is already extracted as string from get_json_object
        # For nested objects/arrays, get_json_object returns JSON string
        return column

    elif transform_type == 'LOOKUP':
        lookup_map = config.get('map', {})
        # Build CASE WHEN chain
        result = column
        for from_val, to_val in lookup_map.items():
            result = when(column == from_val, lit(to_val)).otherwise(result)
        return result

    elif transform_type == 'EXPRESSION':
        # Spark SQL expression (advanced - use with caution)
        expr_str = config.get('expr', '')
        if expr_str:
            return expr(expr_str)
        return column

    else:
        # Unknown transformation type → use as-is
        return column


# =============================================================================
# CLEANING
# =============================================================================

def _clean_data(df: DataFrame, mappings: list) -> DataFrame:
    """
    Apply standard cleaning rules:
    - Empty strings → NULL
    - Trim whitespace on string fields
    - Standardize known enum fields (severity → UPPER)
    """
    # Convert empty strings to NULL for all mapped fields
    for mapping in mappings:
        target = mapping.target_field
        if target in df.columns:
            df = df.withColumn(
                target,
                when(col(target) == '', None).otherwise(col(target))
            )
            # Trim whitespace
            df = df.withColumn(
                target,
                when(col(target).isNotNull(), trim(col(target))).otherwise(col(target))
            )

    # Standardize severity to UPPER if present
    if 'severity' in df.columns:
        df = df.withColumn('severity', upper(col('severity')))

    # Standardize status to UPPER if present
    if 'status' in df.columns:
        df = df.withColumn('status', upper(col('status')))

    return df


# =============================================================================
# VALIDATION
# =============================================================================

def _validate_data(df: DataFrame, silver_domain: str) -> DataFrame:
    """Add DQ flag columns based on Silver domain."""

    if silver_domain == 'vulnerability_intel':
        df = df \
            .withColumn("dq_has_cvss",
                        col("cvss_base_score").isNotNull()) \
            .withColumn("dq_has_epss",
                        col("epss_score").isNotNull() if "epss_score" in df.columns else lit(False)) \
            .withColumn("dq_has_description",
                        col("description").isNotNull() & (length(col("description")) > 0)) \
            .withColumn("dq_completeness_score",
                        _compute_completeness(df, [
                            "cve_id", "cvss_base_score", "severity",
                            "description", "published_date"
                        ]))

    elif silver_domain == 'vulnerability_findings':
        df = df \
            .withColumn("dq_has_cve",
                        col("cve_id").isNotNull() & (col("cve_id") != '')) \
            .withColumn("dq_has_asset",
                        (col("asset_ip").isNotNull()) | (col("asset_hostname").isNotNull())
                        if "asset_ip" in df.columns else lit(False)) \
            .withColumn("dq_severity_valid",
                        col("severity").isin("CRITICAL", "HIGH", "MEDIUM", "LOW", "INFORMATIONAL")
                        if "severity" in df.columns else lit(True))

    elif silver_domain == 'assets':
        df = df \
            .withColumn("dq_has_ip",
                        col("ip_address").isNotNull() if "ip_address" in df.columns else lit(False)) \
            .withColumn("dq_has_hostname",
                        col("hostname").isNotNull() if "hostname" in df.columns else lit(False)) \
            .withColumn("dq_has_owner",
                        col("owner").isNotNull() if "owner" in df.columns else lit(False)) \
            .withColumn("dq_has_criticality",
                        col("criticality").isNotNull() if "criticality" in df.columns else lit(False)) \
            .withColumn("dq_completeness_score",
                        _compute_completeness(df, [
                            "ip_address", "hostname", "os_family",
                            "business_unit", "criticality", "owner"
                        ]))

    return df


def _compute_completeness(df: DataFrame, fields: list):
    """Compute completeness score (0.0-1.0) based on non-null field count."""
    existing_fields = [f for f in fields if f in df.columns]
    if len(existing_fields) == 0:
        return lit(0.0)

    total = len(existing_fields)
    non_null_count = sum([
        when(col(f).isNotNull() & (col(f) != ''), lit(1)).otherwise(lit(0))
        for f in existing_fields
    ])
    return (non_null_count / total).cast(DoubleType())


# =============================================================================
# WRITE STRATEGIES
# =============================================================================

def _write_merge(spark, source_df: DataFrame, target_table: str, merge_key: str, db_silver: str):
    """
    MERGE (upsert) into Silver table.
    - Match on merge_key (e.g., cve_id)
    - If exists → UPDATE
    - If not exists → INSERT
    """
    # Register source as temp view
    source_df.createOrReplaceTempView("silver_source_batch")

    # Get columns from source dataframe (excluding those not in target)
    source_columns = source_df.columns

    # Build SET clause for UPDATE (all columns except merge key)
    update_cols = [c for c in source_columns if c != merge_key]
    set_clause = ", ".join([f"target.{c} = source.{c}" for c in update_cols])

    # Build INSERT columns/values
    insert_cols = ", ".join(source_columns)
    insert_vals = ", ".join([f"source.{c}" for c in source_columns])

    merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING silver_source_batch AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    logger.info(f"Executing MERGE on {merge_key}")
    spark.sql(merge_sql)


# =============================================================================
# CONTROL TABLE OPERATIONS
# =============================================================================

def _update_batch_status(spark, db_control: str, batch_id: str,
                         status: str, records: int, error: str = None):
    """Update batch_registry status."""
    sets = [
        f"batch_status = '{status}'",
        f"records_processed = {records}",
        "end_time = current_timestamp()"
    ]
    if error:
        safe_error = error.replace("'", "''")[:500]
        sets.append(f"failure_reason = '{safe_error}'")
        sets.append("failure_category = 'INTERNAL_ERROR'")

    spark.sql(f"""
        UPDATE {db_control}.t01_ueh_ctl_batch_registry
        SET {', '.join(sets)}
        WHERE batch_id = '{batch_id}'
    """)
    logger.info(f"Batch status updated → {status}")


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    main()
