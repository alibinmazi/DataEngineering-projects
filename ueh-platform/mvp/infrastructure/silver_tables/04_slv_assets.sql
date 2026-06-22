-- =============================================================================
-- UEH Silver Table: Asset Inventory
-- =============================================================================
-- Domain: Normalized asset master (ALL categories in ONE table)
-- Fed by: BMC ADDM, CMDB, scanner-discovered assets (Tenable)
-- Strategy: MERGE on asset_id (latest state wins)
-- Purpose: "What assets exist? What's their config and ownership?"
--
-- Key design:
--   - Single table for all asset types (COMPUTE, CONTAINER, NETWORK, DB, APP)
--   - Common fields as typed columns (queryable)
--   - Category-specific fields in asset_attributes_json (flexible)
--   - Can evolve: split into sub-tables later if query patterns demand it
-- =============================================================================

USE t01_ueh_dev_slv;

CREATE TABLE IF NOT EXISTS t01_ueh_slv_assets (

    -- ─── Universal Identity ──────────────────────────────────────────
    asset_id                STRING      NOT NULL
        COMMENT 'UEH-generated universal asset ID. Primary key.',
    source_asset_id         STRING
        COMMENT 'Original asset ID from source.',
    source_system           STRING      NOT NULL
        COMMENT 'Primary source: BMC_ADDM, CMDB, TENABLE.',

    -- ─── Asset Classification ────────────────────────────────────────
    asset_type              STRING
        COMMENT 'COMPUTE, APPLICATION, DATABASE, NETWORK, CONTAINER, CLOUD_VM, OTHER.',
    asset_subtype           STRING
        COMMENT 'Specific: PHYSICAL_SERVER, VIRTUAL_SERVER, LAPTOP, ROUTER, K8S_POD, RDS, etc.',

    -- ─── Network Identity (Common) ───────────────────────────────────
    ip_address              STRING
        COMMENT 'Primary IP address.',
    ip_addresses_json       STRING
        COMMENT 'All known IPs as JSON array.',
    hostname                STRING
        COMMENT 'Primary hostname.',
    fqdn                    STRING
        COMMENT 'Fully qualified domain name.',
    mac_address             STRING
        COMMENT 'Primary MAC address.',

    -- ─── System Details ──────────────────────────────────────────────
    operating_system        STRING
        COMMENT 'OS name and version.',
    os_family               STRING
        COMMENT 'Normalized: WINDOWS, LINUX, MACOS, NETWORK_OS, CONTAINER_OS, OTHER.',

    -- ─── Business Context ────────────────────────────────────────────
    business_unit           STRING,
    environment             STRING
        COMMENT 'PRODUCTION, STAGING, DEVELOPMENT, DMZ, DR.',
    location                STRING,
    region                  STRING
        COMMENT 'Geographic: us-east, eu-west, ap-southeast.',
    criticality             STRING
        COMMENT 'CRITICAL, HIGH, MEDIUM, LOW.',
    owner                   STRING,
    application_name        STRING
        COMMENT 'Application this asset belongs to.',

    -- ─── Category-Specific Attributes (Flexible) ─────────────────────
    asset_attributes_json   STRING
        COMMENT 'Type-specific details as JSON. Schema varies by asset_type.',

    -- ─── Lifecycle ───────────────────────────────────────────────────
    first_seen              TIMESTAMP,
    last_seen               TIMESTAMP,
    is_active               BOOLEAN     DEFAULT TRUE,
    decommissioned_at       TIMESTAMP,

    -- ─── Source Tracking ─────────────────────────────────────────────
    source_systems_json     STRING
        COMMENT 'All sources reporting this asset: ["BMC_ADDM","TENABLE"].',
    last_source_batch_id    STRING,

    -- ─── UEH Metadata ────────────────────────────────────────────────
    adapter_instance_id     STRING      NOT NULL,
    batch_id                STRING      NOT NULL,
    ingestion_date          DATE        NOT NULL
        COMMENT 'Partition key.',

    -- ─── Data Quality ────────────────────────────────────────────────
    dq_has_ip               BOOLEAN,
    dq_has_hostname         BOOLEAN,
    dq_has_owner            BOOLEAN,
    dq_has_criticality      BOOLEAN,
    dq_completeness_score   DOUBLE
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.mode' = 'merge-on-read',
    'comment' = 'Silver: Asset master. MERGE on asset_id. All types in one table. Category extras in JSON.'
);
