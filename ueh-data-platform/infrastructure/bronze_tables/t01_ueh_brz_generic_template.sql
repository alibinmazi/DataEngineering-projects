-- =============================================================================
-- UEH Bronze Table: Generic Template
-- =============================================================================
-- This template is used when a new source_system is onboarded.
-- The platform creates a Bronze table per source_system using this pattern.
--
-- Table naming: t01_ueh_brz_{source_system_lower}_raw
-- Example:      t01_ueh_brz_nvd_raw, t01_ueh_brz_tenable_raw
--
-- ALL Bronze tables have the IDENTICAL schema — only table name differs.
-- This is what makes the generic Bronze loader possible.
-- =============================================================================

-- Example for NVD (replace {SOURCE} with actual source system)
-- CREATE TABLE IF NOT EXISTS t01_ueh_dev_brz.t01_ueh_brz_{SOURCE}_raw (

CREATE TABLE IF NOT EXISTS t01_ueh_dev_brz.t01_ueh_brz_nvd_raw (

    batch_id                STRING      NOT NULL
        COMMENT 'FK → batch_registry.',
    adapter_instance_id     STRING      NOT NULL
        COMMENT 'Which adapter instance produced this.',
    ingestion_timestamp     TIMESTAMP   NOT NULL
        COMMENT 'When written to Bronze.',
    ingestion_date          DATE        NOT NULL
        COMMENT 'Logical date (partition key).',

    payload_json            STRING      NOT NULL
        COMMENT 'Complete raw source record. Silver parses this using field_mapping.',

    source_record_id        STRING
        COMMENT 'Natural ID from source (CVE-ID, finding_id, etc). Dedup only.',
    chunk_file              STRING
        COMMENT 'Source chunk filename.',
    record_index_in_chunk   INT
        COMMENT 'Position within chunk.',

    ueh_schema_version      STRING      NOT NULL
        COMMENT 'Bronze schema version for replay compatibility.',

    dq_payload_size_bytes   INT
        COMMENT 'Payload size for anomaly detection.'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);
