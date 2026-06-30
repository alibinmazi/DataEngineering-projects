"""
=============================================================================
UEH Platform: Base Bronze Loader (Reusable Framework)
=============================================================================
Purpose: Abstract base class for all Bronze loaders.
         Provides common functionality so adapter-specific loaders
         only need to implement source-specific parsing logic.

Usage:   Subclass this and implement `transform_to_bronze_records()`

Design:  - Reads batch context from control tables
         - Handles status transitions (RAW_COMPLETE → BRONZE_COMPLETE)
         - Manages failure handling and dead-letter registration
         - Provides consistent DQ flag computation
=============================================================================
"""

from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp, lit, length, col
)
from datetime import datetime
import sys
import traceback


class BaseBronzeLoader(ABC):
    """
    Base class for all UEH Bronze loaders.
    
    Subclasses must implement:
        - transform_to_bronze_records(raw_df, batch_context) -> DataFrame
    
    The base class handles:
        - Spark session management
        - Control table reads (batch_registry)
        - Status transitions
        - Error handling and dead-letter registration
        - DQ flag computation
    """

    def __init__(self, adapter_name: str, bronze_table: str):
        """
        Args:
            adapter_name: Name of the adapter (e.g., 'nvd', 'epss', 'tenable')
            bronze_table: Full Iceberg table name (e.g., 't01_ueh_brz_nvd_vulnerabilities')
        """
        self.adapter_name = adapter_name
        self.bronze_table = bronze_table
        self.spark = None
        self.env = None
        self.db_control = None
        self.db_bronze = None

    def initialize_spark(self, app_name: str) -> SparkSession:
        """Create or get SparkSession with Iceberg configuration."""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.spark_catalog.type", "hive") \
            .getOrCreate()
        
        self.env = self.spark.conf.get("ueh.environment", "dev")
        self.db_control = f"ueh_{self.env}_control"
        self.db_bronze = f"ueh_{self.env}_bronze"
        
        return self.spark

    def get_batch_context(self, batch_id: str) -> dict:
        """
        Read batch context from control table.
        
        Returns dict with: batch_id, adapter_instance_id, ingestion_date,
                          load_type, bronze_path, status
        
        Raises: Exception if batch not found or not in RAW_COMPLETE status
        """
        batch_row = self.spark.sql(f"""
            SELECT 
                batch_id,
                adapter_instance_id,
                adapter_name,
                org_id,
                ingestion_date,
                load_type,
                bronze_path,
                status,
                records_ingested,
                watermark_start,
                watermark_end
            FROM {self.db_control}.t01_ueh_ctl_batch_registry
            WHERE batch_id = '{batch_id}'
        """).first()

        if batch_row is None:
            raise Exception(f"Batch '{batch_id}' not found in batch_registry")

        if batch_row.status != 'RAW_COMPLETE':
            raise Exception(
                f"Batch '{batch_id}' is in status '{batch_row.status}', "
                f"expected 'RAW_COMPLETE'. Cannot proceed with Bronze load."
            )

        return batch_row.asDict()

    def update_batch_status(self, batch_id: str, status: str, 
                            records_count: int = None, 
                            failure_reason: str = None,
                            failure_stage: str = None):
        """Update batch_registry status and timestamps."""
        
        timestamp_field = {
            'BRONZE_COMPLETE': 'bronze_completed_at',
            'SILVER_COMPLETE': 'silver_completed_at',
            'GOLD_COMPLETE': 'gold_completed_at',
            'FAILED': 'updated_at'
        }.get(status, 'updated_at')

        set_clauses = [
            f"status = '{status}'",
            f"{timestamp_field} = current_timestamp()",
            f"updated_at = current_timestamp()"
        ]

        if records_count is not None:
            set_clauses.append(f"records_ingested = {records_count}")
        
        if failure_reason is not None:
            # Escape single quotes in error message
            safe_reason = failure_reason.replace("'", "''")[:1000]
            set_clauses.append(f"failure_reason = '{safe_reason}'")
        
        if failure_stage is not None:
            set_clauses.append(f"failure_stage = '{failure_stage}'")

        update_sql = f"""
            UPDATE {self.db_control}.t01_ueh_ctl_batch_registry
            SET {', '.join(set_clauses)}
            WHERE batch_id = '{batch_id}'
        """
        self.spark.sql(update_sql)

    def register_failure(self, batch_id: str, adapter_instance_id: str,
                         failure_stage: str, failure_reason: str,
                         failure_category: str = 'UNKNOWN',
                         records_affected: int = 0):
        """Register failure in failed_ingestions table."""
        
        safe_reason = failure_reason.replace("'", "''")[:2000]
        failure_id = f"fail_{batch_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        self.spark.sql(f"""
            INSERT INTO {self.db_control}.t01_ueh_ctl_failed_ingestions VALUES (
                '{failure_id}',
                '{batch_id}',
                '{adapter_instance_id}',
                current_timestamp(),
                '{failure_stage}',
                '{safe_reason}',
                '{failure_category}',
                NULL,
                TRUE,
                {records_affected},
                'PENDING',
                NULL,
                NULL,
                NULL,
                NULL,
                current_timestamp(),
                current_timestamp()
            )
        """)

    def add_dq_flags(self, df: DataFrame) -> DataFrame:
        """Add standard data quality flag columns to Bronze DataFrame."""
        return df.withColumn(
            "dq_is_valid_json",
            col("payload_json").isNotNull()
        ).withColumn(
            "dq_has_record_id",
            col("source_record_id").isNotNull() & (col("source_record_id") != "")
        ).withColumn(
            "dq_payload_size_bytes",
            length(col("payload_json"))
        )

    @abstractmethod
    def transform_to_bronze_records(self, raw_df: DataFrame, batch_context: dict) -> DataFrame:
        """
        Transform raw chunk files into Bronze table records.
        
        Each subclass implements source-specific logic:
        - How to read the raw files (JSON structure)
        - How to explode arrays into individual records
        - How to extract source_record_id (for operational dedup only)
        
        Args:
            raw_df: Raw DataFrame read from HDFS chunk files
            batch_context: Dict with batch metadata from control table
            
        Returns:
            DataFrame matching Bronze table schema
        """
        pass

    def run(self, batch_id: str):
        """
        Main execution method. Orchestrates the full Bronze load.
        
        Flow:
            1. Initialize Spark
            2. Read batch context from control table
            3. Read raw chunk files from HDFS
            4. Transform to Bronze records (adapter-specific)
            5. Add DQ flags
            6. Write to Iceberg Bronze table
            7. Update batch_registry → BRONZE_COMPLETE
            
        On failure:
            - Updates batch_registry → FAILED
            - Registers in failed_ingestions table
            - Raises exception for Airflow to catch
        """
        app_name = f"UEH_Bronze_{self.adapter_name}_{batch_id}"
        
        try:
            # 1. Initialize
            self.initialize_spark(app_name)
            print(f"[UEH] Bronze load starting: adapter={self.adapter_name}, batch={batch_id}, env={self.env}")

            # 2. Get batch context
            batch_context = self.get_batch_context(batch_id)
            bronze_path = batch_context['bronze_path']
            adapter_instance_id = batch_context['adapter_instance_id']
            print(f"[UEH] Reading from: {bronze_path}")

            # 3. Read raw chunk files
            raw_df = self.spark.read \
                .option("multiline", "true") \
                .json(f"{bronze_path}/chunk_*.json")
            
            print(f"[UEH] Raw files read successfully")

            # 4. Transform to Bronze records (subclass logic)
            bronze_df = self.transform_to_bronze_records(raw_df, batch_context)

            # 5. Add DQ flags
            bronze_df = self.add_dq_flags(bronze_df)

            # 6. Write to Iceberg
            target_table = f"{self.db_bronze}.{self.bronze_table}"
            bronze_df.writeTo(target_table).append()
            
            record_count = bronze_df.count()
            print(f"[UEH] Written {record_count} records to {target_table}")

            # 7. Update status → BRONZE_COMPLETE
            self.update_batch_status(
                batch_id=batch_id,
                status='BRONZE_COMPLETE',
                records_count=record_count
            )
            
            print(f"[UEH] Bronze load COMPLETE: {record_count} records, batch={batch_id}")

        except Exception as e:
            error_msg = str(e)
            print(f"[UEH] ERROR: Bronze load FAILED for batch={batch_id}: {error_msg}")
            traceback.print_exc()
            
            # Update batch status
            try:
                self.update_batch_status(
                    batch_id=batch_id,
                    status='FAILED',
                    failure_reason=error_msg,
                    failure_stage='BRONZE_LOAD'
                )
                self.register_failure(
                    batch_id=batch_id,
                    adapter_instance_id=batch_context.get('adapter_instance_id', 'unknown'),
                    failure_stage='BRONZE_LOAD',
                    failure_reason=error_msg,
                    failure_category='PROCESSING'
                )
            except Exception as inner_e:
                print(f"[UEH] CRITICAL: Failed to update failure status: {inner_e}")
            
            raise

        finally:
            if self.spark:
                self.spark.stop()
