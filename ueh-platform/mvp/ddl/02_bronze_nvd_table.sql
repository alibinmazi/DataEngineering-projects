-- =============================================================================
-- UEH MVP: Bronze Table — NVD Vulnerabilities
-- =============================================================================
-- Database: t01_ueh_dev_brz
-- 
-- v4 slimmed design:
--   - batch_id links to all batch context (no per-record duplication)
--   - payload_json holds complete raw record (Silver parses this)
--   - source_record_id for operational dedup only
--   - ueh_schema_version for replay compatibility
--
-- Run AFTER: mvp/ddl/01_control_tables.sql
-- Execution: spark-sql -f mvp/ddl/02_bronze_nvd_table.sql
-- =============================================================================


-- Create Bronze database
CREATE DATABASE IF NOT EXISTS t01_ueh_dev_brz
    COMMENT 'UEH DEV Bronze layer - raw immutable ingestion records'
    LOCATION '/warehouse/dev/bronze';

USE t01_ueh_dev_brz;


-- ─────────────────────────────────────────────────────────────────────────────
-- Bronze Table: NVD Vulnerabilities
-- ─────────────────────────────────────────────────────────────────────────────
-- DESIGN RULES:
--   1. payload_json = COMPLETE raw CVE record (no field extraction here)
--   2. source_record_id (CVE ID) extracted for dedup ONLY, not business logic
--   3. All business parsing belongs in SILVER layer
--   4. APPEND-ONLY — never update/delete historical records
--   5. Schema changes in NVD API do NOT affect this table
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_brz_nvd_vulnerabilities (

    -- Batch linkage (all context available via batch_registry FK)
    batch_id                STRING      NOT NULL
        COMMENT 'FK to t01_ueh_ctl_batch_registry. All batch context accessible via this.',

    adapter_instance_id     STRING      NOT NULL
        COMMENT 'Which adapter instance produced this record.',

    ingestion_timestamp     TIMESTAMP   NOT NULL
        COMMENT 'Exact moment this record was written to Bronze.',

    ingestion_date          DATE        NOT NULL
        COMMENT 'Logical ingestion date (partition key). Explicit for cross-engine compatibility.',


    -- Raw payload (THE core data — Silver will parse this)
    payload_json            STRING      NOT NULL
        COMMENT 'Complete raw CVE record as JSON string. DO NOT parse in Bronze.',


    -- Operational reference (NOT business logic)
    source_record_id        STRING
        COMMENT 'CVE ID (e.g., CVE-2024-12345). Extracted for dedup/reconciliation ONLY.',

    chunk_file              STRING
        COMMENT 'Source chunk filename for file-level lineage.',

    record_index_in_chunk   INT
        COMMENT 'Zero-based position within chunk for deterministic ordering.',


    -- Schema versioning
    ueh_schema_version      STRING      NOT NULL
        COMMENT 'UEH Bronze schema version (e.g., nvd_brz_v1). For replay compatibility.',


    -- Lightweight DQ
    dq_payload_size_bytes   INT
        COMMENT 'Payload size in bytes. For anomaly detection.'

)
USING iceberg

PARTITIONED BY (ingestion_date)

TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20',
    'comment' = 'UEH Bronze layer: Raw NVD CVE records. Immutable, append-only. Complete API responses stored as payload_json.'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Verify
-- ─────────────────────────────────────────────────────────────────────────────
DESCRIBE TABLE t01_ueh_dev_brz.t01_ueh_brz_nvd_vulnerabilities;
