"""
=============================================================================
UEH Parser: BMC ADDM v1
=============================================================================
Parses Bronze ADDM payload_json → Silver Stage 1 typed columns.

Responsible for:
    - ADDM host/device record extraction
    - Hardware details extraction
    - Network identity extraction
    - Business context extraction
    - DQ flag computation

Version: v1
Source API: BMC ADDM REST API
=============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, get_json_object,
    when, lower
)
from pyspark.sql.types import IntegerType, BooleanType, TimestampType, LongType
import logging

logger = logging.getLogger("UEH-Parser-ADDM-v1")

PARSER_VERSION = "addm_parser_v1"


def parse(bronze_df: DataFrame, batch_id: str,
          adapter_instance_id: str, ingestion_date: str) -> DataFrame:
    """
    Parse ADDM Bronze records into Stage 1 typed columns.
    """
    logger.info(f"Parsing ADDM batch: {batch_id}")

    parsed_df = bronze_df.select(
        # ─── Batch Linkage ────────────────────────────────────────────
        lit(batch_id).alias("batch_id"),
        lit(adapter_instance_id).alias("adapter_instance_id"),
        lit(ingestion_date).cast("date").alias("ingestion_date"),

        # ─── ADDM Identity ───────────────────────────────────────────
        get_json_object(col("payload_json"), "$.key").alias("addm_key"),
        get_json_object(col("payload_json"), "$.type").alias("addm_type"),
        get_json_object(col("payload_json"), "$.hostname").alias("hostname"),
        get_json_object(col("payload_json"), "$.#ip").alias("ip_address"),
        get_json_object(col("payload_json"), "$.fqdn").alias("fqdn"),
        get_json_object(col("payload_json"), "$.mac_address").alias("mac_address"),

        # ─── System Details ───────────────────────────────────────────
        get_json_object(col("payload_json"), "$.os").alias("os_full"),
        get_json_object(col("payload_json"), "$.os_class").alias("os_class"),
        get_json_object(col("payload_json"), "$.os_version").alias("os_version"),

        # ─── Hardware ─────────────────────────────────────────────────
        get_json_object(col("payload_json"), "$.vendor").alias("vendor"),
        get_json_object(col("payload_json"), "$.model").alias("model"),
        get_json_object(col("payload_json"), "$.serial").alias("serial_number"),
        get_json_object(col("payload_json"), "$.#cpucount"
                        ).cast(IntegerType()).alias("cpu_count"),
        get_json_object(col("payload_json"), "$.#ram"
                        ).cast(LongType()).alias("ram_mb"),
        get_json_object(col("payload_json"), "$.#disk_total"
                        ).cast(IntegerType()).alias("disk_total_gb"),
        get_json_object(col("payload_json"), "$.virtual"
                        ).cast(BooleanType()).alias("is_virtual"),
        get_json_object(col("payload_json"), "$.hypervisor").alias("hypervisor"),
        get_json_object(col("payload_json"), "$.cluster").alias("cluster"),

        # ─── Business Context ─────────────────────────────────────────
        get_json_object(col("payload_json"), "$.domain").alias("domain"),
        get_json_object(col("payload_json"), "$.location").alias("location"),
        get_json_object(col("payload_json"), "$.business_service").alias("business_service"),
        get_json_object(col("payload_json"), "$.support_group").alias("support_group"),

        # ─── Discovery Timing ─────────────────────────────────────────
        get_json_object(col("payload_json"), "$.first_discovered"
                        ).cast(TimestampType()).alias("first_discovered"),
        get_json_object(col("payload_json"), "$.last_update_success"
                        ).cast(TimestampType()).alias("last_update_success"),
    )

    # ─── DQ Flags ─────────────────────────────────────────────────────
    parsed_df = parsed_df \
        .withColumn("dq_has_key",
                    col("addm_key").isNotNull()) \
        .withColumn("dq_has_hostname_or_ip",
                    col("hostname").isNotNull() | col("ip_address").isNotNull()) \
        .withColumn("dq_has_os",
                    col("os_full").isNotNull() | col("os_class").isNotNull())

    # ─── Processing Metadata ──────────────────────────────────────────
    parsed_df = parsed_df \
        .withColumn("parsed_at", current_timestamp()) \
        .withColumn("parser_version", lit(PARSER_VERSION))

    logger.info(f"ADDM parsing complete: {parsed_df.count()} records")
    return parsed_df
