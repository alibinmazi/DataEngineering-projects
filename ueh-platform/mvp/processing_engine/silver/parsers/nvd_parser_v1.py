"""
=============================================================================
UEH Parser: NVD v1
=============================================================================
Parses Bronze NVD payload_json → Silver Stage 1 typed columns.

Responsible for:
    - NVD-specific nested JSON extraction
    - CVSS v3.1 extraction (with v2 fallback)
    - CWE/weakness extraction
    - Reference URL extraction
    - CPE configuration extraction
    - DQ flag computation

NOT responsible for:
    - Canonical schema mapping (that's Stage 2)
    - Cross-source enrichment (that's Stage 2)
    - Entity resolution (that's Stage 2)

Version: v1
Source API: NVD CVE API v2.0
=============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, get_json_object,
    when, regexp_extract, length
)
from pyspark.sql.types import DoubleType, TimestampType, BooleanType
import logging

logger = logging.getLogger("UEH-Parser-NVD-v1")

PARSER_VERSION = "nvd_parser_v1"


def parse(bronze_df: DataFrame, batch_id: str,
          adapter_instance_id: str, ingestion_date: str) -> DataFrame:
    """
    Parse NVD Bronze records into Stage 1 typed columns.
    
    Args:
        bronze_df: DataFrame with payload_json column (from Bronze)
        batch_id: Current batch identifier
        adapter_instance_id: Adapter instance
        ingestion_date: Logical ingestion date
    
    Returns:
        DataFrame matching t01_ueh_slv_stg_nvd_vulnerability schema
    """
    logger.info(f"Parsing NVD batch: {batch_id}")

    parsed_df = bronze_df.select(
        # ─── Batch Linkage ────────────────────────────────────────────
        lit(batch_id).alias("batch_id"),
        lit(adapter_instance_id).alias("adapter_instance_id"),
        lit(ingestion_date).cast("date").alias("ingestion_date"),

        # ─── Core CVE Fields ──────────────────────────────────────────
        get_json_object(col("payload_json"), "$.cve.id").alias("cve_id"),
        get_json_object(col("payload_json"), "$.cve.sourceIdentifier").alias("source_identifier"),
        get_json_object(col("payload_json"), "$.cve.vulnStatus").alias("vuln_status"),

        # ─── Description (English) ───────────────────────────────────
        get_json_object(col("payload_json"), "$.cve.descriptions[0].value").alias("description_en"),

        # ─── CVSS v3.1 ───────────────────────────────────────────────
        get_json_object(col("payload_json"),
                        "$.cve.metrics.cvssMetricV31[0].cvssData.baseScore"
                        ).cast(DoubleType()).alias("cvss31_base_score"),
        get_json_object(col("payload_json"),
                        "$.cve.metrics.cvssMetricV31[0].cvssData.baseSeverity"
                        ).alias("cvss31_severity"),
        get_json_object(col("payload_json"),
                        "$.cve.metrics.cvssMetricV31[0].cvssData.vectorString"
                        ).alias("cvss31_vector"),
        get_json_object(col("payload_json"),
                        "$.cve.metrics.cvssMetricV31[0].source"
                        ).alias("cvss31_source"),

        # ─── CVSS v2 (fallback) ──────────────────────────────────────
        get_json_object(col("payload_json"),
                        "$.cve.metrics.cvssMetricV2[0].cvssData.baseScore"
                        ).cast(DoubleType()).alias("cvss2_base_score"),
        get_json_object(col("payload_json"),
                        "$.cve.metrics.cvssMetricV2[0].baseSeverity"
                        ).alias("cvss2_severity"),

        # ─── Temporal ─────────────────────────────────────────────────
        get_json_object(col("payload_json"), "$.cve.published"
                        ).cast(TimestampType()).alias("published_date"),
        get_json_object(col("payload_json"), "$.cve.lastModified"
                        ).cast(TimestampType()).alias("last_modified_date"),

        # ─── Structured Extras (JSON preserved for Stage 2) ──────────
        get_json_object(col("payload_json"), "$.cve.references").alias("references_json"),
        get_json_object(col("payload_json"), "$.cve.weaknesses").alias("weaknesses_json"),
        get_json_object(col("payload_json"), "$.cve.configurations").alias("configurations_json"),
    )

    # ─── DQ Flags ─────────────────────────────────────────────────────
    parsed_df = parsed_df \
        .withColumn("dq_has_cve_id",
                    col("cve_id").isNotNull()) \
        .withColumn("dq_has_cvss",
                    col("cvss31_base_score").isNotNull() | col("cvss2_base_score").isNotNull()) \
        .withColumn("dq_has_description",
                    col("description_en").isNotNull() & (length(col("description_en")) > 0)) \
        .withColumn("dq_cve_format_valid",
                    col("cve_id").rlike("^CVE-[0-9]{4}-[0-9]{4,}$")) \
        .withColumn("dq_cvss_in_range",
                    when(col("cvss31_base_score").isNotNull(),
                         (col("cvss31_base_score") >= 0) & (col("cvss31_base_score") <= 10))
                    .otherwise(lit(True)))

    # ─── Processing Metadata ──────────────────────────────────────────
    parsed_df = parsed_df \
        .withColumn("parsed_at", current_timestamp()) \
        .withColumn("parser_version", lit(PARSER_VERSION))

    logger.info(f"NVD parsing complete: {parsed_df.count()} records")
    return parsed_df
