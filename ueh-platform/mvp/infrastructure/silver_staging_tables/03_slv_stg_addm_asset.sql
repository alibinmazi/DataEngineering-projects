-- =============================================================================
-- Silver Stage 1: BMC ADDM Asset Staging
-- =============================================================================
-- Purpose: Typed, parsed ADDM asset records. Still ADDM-specific schema.
--          NOT canonical yet — that happens in Stage 2.
--
-- Fed by: t01_ueh_brz_bmc_addm_raw (Bronze)
-- Feeds into: t01_ueh_slv_asset (Canonical Stage 2)
-- Write strategy: APPEND (per batch)
--
-- What happens here:
--   - payload_json → typed columns
--   - ADDM internal key preserved
--   - Hardware details extracted
--   - Network identity extracted
--   - Business context extracted
--   - Adapter-local dedup (same key in same batch = keep latest)
-- =============================================================================

USE t01_ueh_dev_slv;

CREATE TABLE IF NOT EXISTS t01_ueh_slv_stg_addm_asset (

    -- ─── Batch Linkage ───────────────────────────────────────────────
    batch_id                STRING      NOT NULL,
    adapter_instance_id     STRING      NOT NULL,
    ingestion_date          DATE        NOT NULL,

    -- ─── ADDM-Specific Asset Fields ──────────────────────────────────
    addm_key                STRING
        COMMENT 'ADDM internal identifier (HOST-abc123).',
    addm_type               STRING
        COMMENT 'ADDM type: Host, NetworkDevice, Printer, StorageDevice.',
    hostname                STRING,
    ip_address              STRING,
    fqdn                    STRING,
    mac_address             STRING,

    -- ─── System Details ──────────────────────────────────────────────
    os_full                 STRING
        COMMENT 'Full OS string: Ubuntu 22.04.3 LTS.',
    os_class                STRING
        COMMENT 'ADDM classification: Linux, Windows, UNIX, etc.',
    os_version              STRING,

    -- ─── Hardware ────────────────────────────────────────────────────
    vendor                  STRING,
    model                   STRING,
    serial_number           STRING,
    cpu_count               INT,
    ram_mb                  BIGINT,
    disk_total_gb           INT,
    is_virtual              BOOLEAN,
    hypervisor              STRING,
    cluster                 STRING,

    -- ─── Business Context ────────────────────────────────────────────
    domain                  STRING,
    location                STRING,
    business_service        STRING,
    support_group           STRING,

    -- ─── Discovery Timing ────────────────────────────────────────────
    first_discovered        TIMESTAMP,
    last_update_success     TIMESTAMP,

    -- ─── DQ Flags (Stage 1 validation) ───────────────────────────────
    dq_has_key              BOOLEAN,
    dq_has_hostname_or_ip   BOOLEAN,
    dq_has_os               BOOLEAN,

    -- ─── Processing Metadata ─────────────────────────────────────────
    parsed_at               TIMESTAMP,
    parser_version          STRING
        COMMENT 'Parser class version: addm_parser_v1'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);
