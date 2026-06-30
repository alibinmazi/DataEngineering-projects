"""
=============================================================================
UEH Parser: Tenable v1
=============================================================================
Parses Bronze Tenable payload_json → Silver Stage 1 typed columns.

Responsible for:
    - Tenable export chunk nested JSON extraction
    - Plugin details extraction
    - Asset context extraction
    - VPR score extraction
    - CVE list extraction (array handling)
    - Port/protocol extraction
    - DQ flag computation

Version: v1
Source API: Tenable.io Vuln Export API
=============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, get_json_object,
    when, length
)
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
import logging

logger = logging.getLogger("UEH-Parser-Tenable-v1")

PARSER_VERSION = "tenable_parser_v1"


def parse(bronze_df: DataFrame, batch_id: str,
          adapter_instance_id: str, ingestion_date: str) -> DataFrame:
    """
    Parse Tenable Bronze records into Stage 1 typed columns.
    """
    logger.info(f"Parsing Tenable batch: {batch_id}")

    parsed_df = bronze_df.select(
        # ─── Batch Linkage ────────────────────────────────────────────
        lit(batch_id).alias("batch_id"),
        lit(adapter_instance_id).alias("adapter_instance_id"),
        lit(ingestion_date).cast("date").alias("ingestion_date"),

        # ─── Plugin (Vulnerability) Details ───────────────────────────
        get_json_object(col("payload_json"), "$.plugin.id"
                        ).cast(IntegerType()).alias("plugin_id"),
        get_json_object(col("payload_json"), "$.plugin.name"
                        ).alias("plugin_name"),
        get_json_object(col("payload_json"), "$.plugin.family"
                        ).alias("plugin_family"),
        get_json_object(col("payload_json"), "$.severity"
                        ).cast(IntegerType()).alias("severity_id"),
        get_json_object(col("payload_json"), "$.plugin.cvss_base_score"
                        ).cast(DoubleType()).alias("cvss_base_score"),
        get_json_object(col("payload_json"), "$.plugin.vpr.score"
                        ).cast(DoubleType()).alias("vpr_score"),

        # CVE list (full array as JSON)
        get_json_object(col("payload_json"), "$.plugin.cve"
                        ).alias("cve_list_json"),
        # Primary CVE (first in list)
        get_json_object(col("payload_json"), "$.plugin.cve[0]"
                        ).alias("primary_cve"),

        # ─── Asset Context ────────────────────────────────────────────
        get_json_object(col("payload_json"), "$.asset.uuid").alias("asset_uuid"),
        get_json_object(col("payload_json"), "$.asset.hostname").alias("asset_hostname"),
        get_json_object(col("payload_json"), "$.asset.ipv4").alias("asset_ip"),
        get_json_object(col("payload_json"), "$.asset.fqdn").alias("asset_fqdn"),
        get_json_object(col("payload_json"), "$.asset.operating_system[0]").alias("asset_os"),
        get_json_object(col("payload_json"), "$.asset.network_id").alias("asset_network_id"),

        # ─── Finding State ────────────────────────────────────────────
        get_json_object(col("payload_json"), "$.state").alias("state"),
        get_json_object(col("payload_json"), "$.first_found"
                        ).cast(TimestampType()).alias("first_found"),
        get_json_object(col("payload_json"), "$.last_found"
                        ).cast(TimestampType()).alias("last_found"),

        # ─── Network Context ──────────────────────────────────────────
        get_json_object(col("payload_json"), "$.port.port"
                        ).cast(IntegerType()).alias("port"),
        get_json_object(col("payload_json"), "$.port.protocol").alias("protocol"),
        get_json_object(col("payload_json"), "$.port.service").alias("service"),

        # ─── Detail ───────────────────────────────────────────────────
        get_json_object(col("payload_json"), "$.output").alias("output"),
        get_json_object(col("payload_json"), "$.plugin.solution").alias("solution"),
    )

    # ─── DQ Flags ─────────────────────────────────────────────────────
    parsed_df = parsed_df \
        .withColumn("dq_has_plugin_id",
                    col("plugin_id").isNotNull()) \
        .withColumn("dq_has_asset",
                    col("asset_uuid").isNotNull() | col("asset_ip").isNotNull()) \
        .withColumn("dq_severity_valid",
                    col("severity_id").between(0, 4)) \
        .withColumn("dq_has_state",
                    col("state").isNotNull() & (col("state") != ''))

    # ─── Processing Metadata ──────────────────────────────────────────
    parsed_df = parsed_df \
        .withColumn("parsed_at", current_timestamp()) \
        .withColumn("parser_version", lit(PARSER_VERSION))

    logger.info(f"Tenable parsing complete: {parsed_df.count()} records")
    return parsed_df
