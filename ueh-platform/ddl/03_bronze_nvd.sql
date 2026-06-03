-- =============================================================================
-- UEH Platform: Bronze Table - NVD Vulnerabilities
-- =============================================================================
-- Purpose: Store raw NVD CVE records as immutable payload_json
-- Source: National Vulnerability Database (NVD) API v2.0
-- API: https://services.nvd.nist.gov/rest/json/cves/2.0
--
-- CRITICAL DESIGN RULES:
-- 1. payload_json stores the COMPLETE raw CVE record — no field extraction
-- 2. source_record_id (CVE ID) is extracted ONLY for operational dedup, NOT business logic
-- 3. All business parsing belongs in SILVER layer
-- 4. This table is APPEND-ONLY — never update/delete historical records
-- 5. Schema changes in NVD API do NOT affect this table (payload_json absorbs all)
--
-- Execution: spark-sql -f ddl/03_bronze_nvd.sql
-- =============================================================================

USE ueh_dev_bronze;

-- ─────────────────────────────────────────────────────────────────────────────
-- Bronze Table: NVD Vulnerabilities
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_brz_nvd_vulnerabilities (
    -- ─── Ingestion Metadata ───────────────────────────────────────────────────
    batch_id                STRING      NOT NULL    COMMENT 'FK to batch_registry. Links to complete batch context.',
    adapter_instance_id     STRING      NOT NULL    COMMENT 'FK to adapter_config. Which adapter instance produced this.',
    adapter_name            STRING      NOT NULL    COMMENT 'Adapter name (always "nvd" for this table)',
    ingestion_timestamp     TIMESTAMP   NOT NULL    COMMENT 'Exact moment this record was written to Bronze',
    ingestion_date          DATE        NOT NULL    COMMENT 'Logical ingestion date (partition key)',
    load_type               STRING      NOT NULL    COMMENT 'FULL_LOAD, INCREMENTAL, REPLAY — what this run executed',
    source_api_endpoint     STRING                  COMMENT 'Full API URL used for this ingestion',
    source_api_version      STRING                  COMMENT 'API version (e.g., 2.0)',
    
    -- ─── Raw Payload ──────────────────────────────────────────────────────────
    -- This is the COMPLETE source record. Silver layer will parse this.
    -- Bronze NEVER fails due to schema changes because this absorbs everything.
    payload_json            STRING      NOT NULL    COMMENT 'Complete raw CVE record as JSON string. DO NOT parse in Bronze.',
    
    -- ─── Operational Metadata ─────────────────────────────────────────────────
    -- These fields exist for operational purposes only (file lineage, dedup checks)
    chunk_file              STRING                  COMMENT 'Source chunk filename (e.g., chunk_001.json)',
    record_index_in_chunk   INT                     COMMENT 'Zero-based position of this record within its chunk',
    source_record_id        STRING                  COMMENT 'CVE ID (e.g., CVE-2024-12345). Extracted for dedup ONLY, not business logic.',
    
    -- ─── Data Quality Flags ───────────────────────────────────────────────────
    dq_is_valid_json        BOOLEAN     DEFAULT TRUE COMMENT 'Whether payload_json is valid JSON',
    dq_has_record_id        BOOLEAN     DEFAULT TRUE COMMENT 'Whether source_record_id could be extracted',
    dq_payload_size_bytes   INT                     COMMENT 'Size of payload_json in bytes'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20',
    'write.parquet.compression-codec' = 'zstd',
    'commit.retry.num-retries' = '3'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Optional: Table comment for documentation
-- ─────────────────────────────────────────────────────────────────────────────
COMMENT ON TABLE t01_ueh_brz_nvd_vulnerabilities IS 
    'Bronze layer: Raw NVD CVE records. Immutable, append-only. Complete API responses stored as payload_json. All field extraction happens in Silver.';


-- =============================================================================
-- Verification
-- =============================================================================
DESCRIBE EXTENDED ueh_dev_bronze.t01_ueh_brz_nvd_vulnerabilities;
