-- =============================================================================
-- UEH MVP: Bronze Table — NVD Vulnerabilities
-- =============================================================================
-- Minimal Bronze table following v4 slimmed design:
--   - batch_id links to all batch context (no duplication)
--   - payload_json holds complete raw record
--   - source_record_id for operational dedup only
--   - ueh_schema_version for replay compatibility
--
-- Run AFTER: mvp/ddl/01_minimal_control_tables.sql
-- Execution: spark-sql -f mvp/ddl/02_bronze_nvd_table.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ueh_dev_bronze
    LOCATION '/warehouse/dev/bronze';

USE ueh_dev_bronze;

CREATE TABLE IF NOT EXISTS t01_ueh_brz_nvd_vulnerabilities (
    -- Batch linkage (all context available via batch_registry FK)
    batch_id                STRING      NOT NULL    COMMENT 'FK to batch_registry',
    adapter_instance_id     STRING      NOT NULL    COMMENT 'Which adapter instance produced this',
    ingestion_timestamp     TIMESTAMP   NOT NULL    COMMENT 'When this record was written to Bronze',
    ingestion_date          DATE        NOT NULL    COMMENT 'Logical date (partition key)',

    -- Raw payload (THE core data — Silver will parse this)
    payload_json            STRING      NOT NULL    COMMENT 'Complete raw CVE record. DO NOT parse here.',

    -- Operational (NOT business logic)
    source_record_id        STRING                  COMMENT 'CVE ID for dedup/reconciliation only',
    chunk_file              STRING                  COMMENT 'Source chunk filename',
    record_index_in_chunk   INT                     COMMENT 'Position within chunk',

    -- Schema versioning
    ueh_schema_version      STRING      NOT NULL    COMMENT 'Bronze schema version: nvd_brz_v1',

    -- Lightweight DQ
    dq_payload_size_bytes   INT                     COMMENT 'Payload size for anomaly detection'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

-- Verify
DESCRIBE TABLE ueh_dev_bronze.t01_ueh_brz_nvd_vulnerabilities;
