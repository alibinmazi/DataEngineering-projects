-- =============================================================================
-- Silver Stage 1: Tenable Finding Staging
-- =============================================================================
-- Purpose: Typed, parsed Tenable export data. Still Tenable-specific schema.
--          NOT canonical yet — that happens in Stage 2.
--
-- Fed by: t01_ueh_brz_tenable_raw (Bronze)
-- Feeds into: t01_ueh_slv_vulnerability_finding (Canonical Stage 2)
-- Write strategy: APPEND (per batch)
--
-- What happens here:
--   - payload_json → typed columns
--   - Severity numeric (0-4) → kept as-is (canonical maps to enum in Stage 2)
--   - Plugin CVE array → extracted first CVE
--   - Asset details extracted
--   - VPR score extracted
--   - Port/protocol extracted
--   - Adapter-local dedup (same plugin+asset in same batch = keep latest)
-- =============================================================================

USE t01_ueh_dev_slv;

CREATE TABLE IF NOT EXISTS t01_ueh_slv_stg_tenable_finding (

    -- ─── Batch Linkage ───────────────────────────────────────────────
    batch_id                STRING      NOT NULL,
    adapter_instance_id     STRING      NOT NULL,
    ingestion_date          DATE        NOT NULL,

    -- ─── Tenable-Specific Finding Fields ─────────────────────────────
    plugin_id               INT,
    plugin_name             STRING,
    plugin_family           STRING,
    severity_id             INT
        COMMENT 'Tenable numeric: 0=Info, 1=Low, 2=Med, 3=High, 4=Critical',
    cvss_base_score         DOUBLE,
    vpr_score               DOUBLE
        COMMENT 'Tenable Vulnerability Priority Rating.',
    cve_list_json           STRING
        COMMENT 'Array of CVE IDs: ["CVE-2024-1234","CVE-2024-5678"]',
    primary_cve             STRING
        COMMENT 'First CVE from list (for quick reference).',

    -- ─── Asset Context ───────────────────────────────────────────────
    asset_uuid              STRING,
    asset_hostname          STRING,
    asset_ip                STRING,
    asset_fqdn              STRING,
    asset_os                STRING,
    asset_network_id        STRING,

    -- ─── Finding State ───────────────────────────────────────────────
    state                   STRING
        COMMENT 'Tenable state: open, reopened, fixed.',
    first_found             TIMESTAMP,
    last_found              TIMESTAMP,

    -- ─── Network Context ─────────────────────────────────────────────
    port                    INT,
    protocol                STRING,
    service                 STRING,

    -- ─── Detail ──────────────────────────────────────────────────────
    output                  STRING
        COMMENT 'Scanner proof (may be large).',
    solution                STRING,

    -- ─── DQ Flags (Stage 1 validation) ───────────────────────────────
    dq_has_plugin_id        BOOLEAN,
    dq_has_asset            BOOLEAN,
    dq_severity_valid       BOOLEAN
        COMMENT 'TRUE if severity_id between 0 and 4.',
    dq_has_state            BOOLEAN,

    -- ─── Processing Metadata ─────────────────────────────────────────
    parsed_at               TIMESTAMP,
    parser_version          STRING
        COMMENT 'Parser class version: tenable_parser_v1'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);
