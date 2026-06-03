"""
=============================================================================
UEH Platform: Bronze Loader - NVD (National Vulnerability Database)
=============================================================================
Purpose: Load raw NVD CVE JSON chunks from HDFS into Bronze Iceberg table.

Source:  NVD API v2.0 (https://services.nvd.nist.gov/rest/json/cves/2.0)

API Response Structure:
    {
        "resultsPerPage": 2000,
        "startIndex": 0,
        "totalResults": 25000,
        "format": "NVD_CVE",
        "version": "2.0",
        "timestamp": "2026-05-20T03:00:00.000",
        "vulnerabilities": [
            {
                "cve": {
                    "id": "CVE-2024-12345",
                    "sourceIdentifier": "...",
                    "published": "...",
                    "lastModified": "...",
                    "descriptions": [...],
                    "metrics": {...},
                    "configurations": [...],
                    "references": [...]
                }
            },
            ...
        ]
    }

Design Decisions:
    - Each element in "vulnerabilities" array becomes ONE Bronze record
    - payload_json = complete "vulnerabilities[N]" object (includes "cve" wrapper)
    - source_record_id = CVE ID (e.g., "CVE-2024-12345") — extracted for dedup ONLY
    - NO business field parsing in Bronze — that belongs in Silver

Execution:
    spark-submit --conf ueh.environment=dev \
        spark/bronze/bronze_nvd_loader.py <batch_id>

    Or via CDE:
    cde spark submit --conf ueh.environment=dev \
        --application-file spark/bronze/bronze_nvd_loader.py \
        -- <batch_id>
=============================================================================
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    explode, col, lit, current_timestamp, 
    input_file_name, monotonically_increasing_id,
    get_json_object, to_json, struct
)
from pyspark.sql.types import StringType
import sys

# Import base class
from base_bronze_loader import BaseBronzeLoader


class NVDBronzeLoader(BaseBronzeLoader):
    """
    NVD-specific Bronze loader.
    
    Handles the NVD API response structure where CVE records are nested
    inside a "vulnerabilities" array. Each array element becomes one
    Bronze table record with the complete JSON preserved as payload_json.
    """

    def __init__(self):
        super().__init__(
            adapter_name='nvd',
            bronze_table='t01_ueh_brz_nvd_vulnerabilities'
        )

    def transform_to_bronze_records(self, raw_df: DataFrame, batch_context: dict) -> DataFrame:
        """
        Transform NVD raw chunk files into Bronze records.
        
        NVD chunk files contain the full API response including:
        - resultsPerPage, startIndex, totalResults (pagination metadata)
        - vulnerabilities[] array (the actual CVE records)
        
        We explode the vulnerabilities array so each CVE = one Bronze record.
        """
        
        batch_id = batch_context['batch_id']
        adapter_instance_id = batch_context['adapter_instance_id']
        ingestion_date = batch_context['ingestion_date']
        load_type = batch_context['load_type']

        # ─── Step 1: Explode vulnerabilities array ─────────────────────────────
        # Each chunk file has a "vulnerabilities" array with CVE objects
        # We need one row per CVE record
        
        exploded_df = raw_df.select(
            input_file_name().alias("_source_file"),
            explode(col("vulnerabilities")).alias("vuln_record")
        )

        # ─── Step 2: Build Bronze record ──────────────────────────────────────
        # Convert the struct to JSON string for payload_json
        # Extract only CVE ID for operational reference (NOT business parsing)
        
        bronze_df = exploded_df.select(
            # Ingestion metadata
            lit(batch_id).alias("batch_id"),
            lit(adapter_instance_id).alias("adapter_instance_id"),
            lit("nvd").alias("adapter_name"),
            current_timestamp().alias("ingestion_timestamp"),
            lit(str(ingestion_date)).cast("date").alias("ingestion_date"),
            lit(load_type).alias("load_type"),
            lit("https://services.nvd.nist.gov/rest/json/cves/2.0").alias("source_api_endpoint"),
            lit("2.0").alias("source_api_version"),
            
            # Raw payload — COMPLETE record as JSON string
            # This is the ONLY place the data lives in Bronze
            to_json(col("vuln_record")).alias("payload_json"),
            
            # Operational metadata
            col("_source_file").alias("chunk_file"),
            monotonically_increasing_id().cast("int").alias("record_index_in_chunk"),
            
            # Source record ID — extracted ONLY for dedup/lookup, NOT business logic
            # NVD structure: vuln_record.cve.id = "CVE-2024-12345"
            col("vuln_record.cve.id").alias("source_record_id")
        )

        return bronze_df


# ─────────────────────────────────────────────────────────────────────────────
# Main Entry Point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """
    CLI entry point for NVD Bronze loader.
    
    Usage: spark-submit bronze_nvd_loader.py <batch_id>
    
    Args:
        batch_id: The batch to process (must be in RAW_COMPLETE status)
    """
    if len(sys.argv) < 2:
        print("Usage: bronze_nvd_loader.py <batch_id>")
        print("Example: bronze_nvd_loader.py batch_20260520030000_nvd_public_01")
        sys.exit(1)
    
    batch_id = sys.argv[1]
    
    loader = NVDBronzeLoader()
    loader.run(batch_id)


if __name__ == "__main__":
    main()
