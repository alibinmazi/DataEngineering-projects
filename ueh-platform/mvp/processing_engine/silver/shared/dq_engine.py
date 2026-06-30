"""
=============================================================================
UEH Silver Shared: Data Quality Engine
=============================================================================
Computes DQ flag columns for Silver records.
Each Silver domain has its own DQ checks.

Used by ALL 3 Silver jobs — call the appropriate function per domain.
=============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, length, coalesce
from pyspark.sql.types import DoubleType
import logging

logger = logging.getLogger("UEH-Silver-DQ")


def add_dq_flags_vulnerability_intel(df: DataFrame) -> DataFrame:
    """DQ flags for slv_vulnerability_intel table."""

    df = df.withColumn("dq_has_cvss",
                       col("cvss_base_score").isNotNull())

    if "epss_score" in df.columns:
        df = df.withColumn("dq_has_epss", col("epss_score").isNotNull())
    else:
        df = df.withColumn("dq_has_epss", lit(False))

    df = df.withColumn("dq_has_description",
                       col("description").isNotNull() & (length(col("description")) > 0))

    # Completeness score
    completeness_fields = ["cve_id", "cvss_base_score", "severity", "description", "published_date"]
    df = _add_completeness_score(df, completeness_fields)

    logger.info("DQ flags added for vulnerability_intel")
    return df


def add_dq_flags_vulnerability_findings(df: DataFrame) -> DataFrame:
    """DQ flags for slv_vulnerability_findings table."""

    df = df.withColumn("dq_has_cve",
                       col("cve_id").isNotNull() & (col("cve_id") != ''))

    # Has asset = at least one of ip/hostname is present
    has_ip = col("asset_ip").isNotNull() if "asset_ip" in df.columns else lit(False)
    has_hostname = col("asset_hostname").isNotNull() if "asset_hostname" in df.columns else lit(False)
    df = df.withColumn("dq_has_asset", has_ip | has_hostname)

    # Severity valid
    valid_severities = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFORMATIONAL"]
    if "severity" in df.columns:
        df = df.withColumn("dq_severity_valid", col("severity").isin(valid_severities))
    else:
        df = df.withColumn("dq_severity_valid", lit(True))

    logger.info("DQ flags added for vulnerability_findings")
    return df


def add_dq_flags_assets(df: DataFrame) -> DataFrame:
    """DQ flags for slv_assets table."""

    df = df.withColumn("dq_has_ip",
                       col("ip_address").isNotNull() if "ip_address" in df.columns else lit(False))

    df = df.withColumn("dq_has_hostname",
                       col("hostname").isNotNull() if "hostname" in df.columns else lit(False))

    df = df.withColumn("dq_has_owner",
                       col("owner").isNotNull() if "owner" in df.columns else lit(False))

    df = df.withColumn("dq_has_criticality",
                       col("criticality").isNotNull() if "criticality" in df.columns else lit(False))

    # Completeness
    completeness_fields = ["ip_address", "hostname", "os_family", "business_unit", "criticality", "owner"]
    df = _add_completeness_score(df, completeness_fields)

    logger.info("DQ flags added for assets")
    return df


def _add_completeness_score(df: DataFrame, fields: list) -> DataFrame:
    """Compute completeness score (0.0 - 1.0) based on non-null fields."""
    existing = [f for f in fields if f in df.columns]
    if not existing:
        return df.withColumn("dq_completeness_score", lit(0.0))

    total = len(existing)

    # Build sum of non-null indicators
    non_null_expr = sum([
        when(col(f).isNotNull() & (col(f) != ''), lit(1.0)).otherwise(lit(0.0))
        for f in existing
    ])

    df = df.withColumn("dq_completeness_score", (non_null_expr / lit(total)).cast(DoubleType()))
    return df
