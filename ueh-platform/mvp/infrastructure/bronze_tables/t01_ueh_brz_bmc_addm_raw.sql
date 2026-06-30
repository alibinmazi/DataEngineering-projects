-- =============================================================================
-- UEH Bronze Table: BMC ADDM Asset Discovery (Raw)
-- =============================================================================
-- Source: BMC Atrium Discovery and Dependency Mapping (REST API)
-- Category: asset_inventory
-- Silver target: slv_assets (business entity)
-- Ingestion: Standard REST pagination (offset-based)
--
-- Same Bronze schema as all adapters (generic pattern):
--   payload_json holds complete raw asset/host record
--   source_record_id = ADDM internal key
--
-- Partition: days(ingestion_ts) only
--   No adapter_instance_id partition (typically single instance for asset inventory)
-- =============================================================================

CREATE TABLE IF NOT EXISTS hive_catalog.t01_ueh_dev_brz.t01_ueh_brz_bmc_addm_raw (

    org_id                  STRING
        COMMENT 'Logical UEH organisation/tenant identifier.',

    adapter_instance_id     STRING      NOT NULL
        COMMENT 'FK → adapter_config. Example: addm_prod_01.',

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
        COMMENT 'Raw chunk/page file name. Example: chunk_001.json',

    page_number             INT
        COMMENT 'Page number from ADDM API pagination.',

    record_index            INT
        COMMENT 'Record position within chunk (0-based).',

    source_record_id        STRING
        COMMENT 'ADDM internal key. Natural source identifier for asset.',

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
        COMMENT 'Complete immutable raw ADDM asset record. Silver parses this.'
)
USING iceberg

PARTITIONED BY (
    days(ingestion_ts)
)

LOCATION 'hdfs:///ueh/warehouse/bronze/t01_ueh_brz_bmc_addm_raw'

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
    'comment' = 'UEH Bronze raw BMC ADDM table. Immutable append-only. One row per discovered asset/host record.'
);
