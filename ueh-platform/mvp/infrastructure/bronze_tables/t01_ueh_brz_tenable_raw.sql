-- =============================================================================
-- UEH Bronze Table: Tenable Vulnerability Findings (Raw)
-- =============================================================================
-- Source: Tenable.io Export API (https://cloud.tenable.com/vulns/export)
-- Category: scanner
-- Silver target: slv_vulnerability_findings (business entity)
-- Ingestion: Async export (POST → poll → download chunks)
--
-- Same Bronze schema as all adapters (generic pattern):
--   payload_json holds complete raw finding record
--   source_record_id = plugin_id (Tenable's natural ID)
--
-- Partition: adapter_instance_id + days(ingestion_ts)
--   Supports multi-instance (tenable_prod_us_01, tenable_prod_eu_01)
-- =============================================================================

CREATE TABLE IF NOT EXISTS hive_catalog.t01_ueh_dev_brz.t01_ueh_brz_tenable_raw (

    org_id                  STRING
        COMMENT 'Logical UEH organisation/tenant identifier.',

    adapter_instance_id     STRING      NOT NULL
        COMMENT 'FK → adapter_config. Example: tenable_prod_us_01.',

    batch_id                STRING      NOT NULL
        COMMENT 'FK → batch_registry. One execution batch.',

    ingestion_ts            TIMESTAMP   NOT NULL
        COMMENT 'UTC timestamp when Bronze record was written.',

    load_type               STRING      NOT NULL
        COMMENT 'FULL_LOAD | INCREMENTAL | SNAPSHOT | REPLAY',

    -- ─────────────────────────────────────────────────────────────
    -- Source Window / Replay Context
    -- ─────────────────────────────────────────────────────────────
    source_file_name        STRING
        COMMENT 'Export chunk filename. Example: chunk_001.json',

    page_number             INT
        COMMENT 'Chunk number from Tenable export.',

    record_index            INT
        COMMENT 'Record position within chunk (0-based).',

    source_record_id        STRING
        COMMENT 'Tenable plugin_id as string. Natural source identifier.',

    -- ─────────────────────────────────────────────────────────────
    -- Payload & Operational Metadata
    -- ─────────────────────────────────────────────────────────────
    payload_hash            STRING
        COMMENT 'SHA-256 hash of payload_json. For dedup/replay validation.',

    payload_size_bytes      BIGINT
        COMMENT 'Raw payload size in bytes.',

    schema_version          STRING
        COMMENT 'UEH parser/schema compatibility version. Example: bronze_v1.',

    payload_json            STRING      NOT NULL
        COMMENT 'Complete immutable raw Tenable finding. Silver parses this.'
)
USING iceberg

PARTITIONED BY (
    adapter_instance_id,
    days(ingestion_ts)
)

LOCATION 'hdfs:///ueh/warehouse/bronze/t01_ueh_brz_tenable_raw'

TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.compression-codec' = 'gzip',
    'write.target-file-size-bytes' = '134217728',
    'read.split.target-size' = '134217728',
    'history.expire.min-snapshots-to-keep' = '10',
    'history.expire.max-snapshot-age-ms' = '2592000000',
    'commit.retry.num-retries' = '3',
    'comment' = 'UEH Bronze raw Tenable table. Immutable append-only. One row per vulnerability finding from Tenable export.'
);
