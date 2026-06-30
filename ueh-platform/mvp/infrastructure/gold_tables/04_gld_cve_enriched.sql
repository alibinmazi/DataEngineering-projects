-- =============================================================================
-- UEH Gold Table: CVE Enriched (Complete CVE Intelligence)
-- =============================================================================
-- Purpose: Single comprehensive view of each CVE with ALL enrichments
-- Combines: NVD base data + EPSS score + CISA KEV status + org exposure count
-- Used by: Chatbot, CVE lookup API, analyst investigation
--
-- One row per CVE (deduplicated, fully enriched)
-- Strategy: OVERWRITE partition daily
-- =============================================================================

USE t01_ueh_dev_gld;

CREATE TABLE IF NOT EXISTS t01_ueh_gld_cve_enriched (

    -- ─── CVE Identity ────────────────────────────────────────────────
    cve_id                  STRING      NOT NULL
        COMMENT 'Primary key: CVE-2024-12345.',

    -- ─── Base Intelligence (from NVD) ────────────────────────────────
    description             STRING,
    severity                STRING,
    cvss_base_score         DOUBLE,
    cvss_version            STRING,
    published_date          TIMESTAMP,
    last_modified_date      TIMESTAMP,
    references_json         STRING,
    affected_products_json  STRING,
    weaknesses_json         STRING,

    -- ─── Exploit Intelligence (from EPSS) ────────────────────────────
    epss_score              DOUBLE
        COMMENT 'Exploit probability (0.0-1.0).',
    epss_percentile         DOUBLE
        COMMENT 'EPSS percentile (0.0-1.0).',
    exploit_likelihood      STRING
        COMMENT 'Derived: VERY_HIGH (>0.9), HIGH (>0.7), MEDIUM (>0.3), LOW.',

    -- ─── KEV Status (from CISA) ──────────────────────────────────────
    is_in_kev               BOOLEAN     DEFAULT FALSE,
    kev_date_added          DATE,
    kev_due_date            DATE,
    is_actively_exploited   BOOLEAN     DEFAULT FALSE,
    days_until_kev_deadline INT
        COMMENT 'Days remaining until CISA deadline (NULL if not in KEV).',

    -- ─── Org Exposure (from findings) ────────────────────────────────
    total_affected_assets   BIGINT
        COMMENT 'How many assets in our org have this CVE open.',
    critical_assets_affected BIGINT
        COMMENT 'How many CRITICAL assets are affected.',
    production_assets_affected BIGINT
        COMMENT 'How many PRODUCTION assets are affected.',
    first_detected_in_org   TIMESTAMP
        COMMENT 'When first found on any org asset.',
    last_detected_in_org    TIMESTAMP
        COMMENT 'When last confirmed on any org asset.',
    days_in_org             INT
        COMMENT 'Days since first detected in our environment.',

    -- ─── Priority Score ──────────────────────────────────────────────
    ueh_priority_score      DOUBLE
        COMMENT 'UEH composite priority (0-100). CVSS + EPSS + KEV + exposure.',
    priority_tier           STRING
        COMMENT 'P1_IMMEDIATE, P2_URGENT, P3_PLANNED, P4_MONITOR.',

    -- ─── Metadata ────────────────────────────────────────────────────
    source_systems_json     STRING
        COMMENT 'Sources that contributed: ["NVD","EPSS","CISA_KEV"].',
    computed_at             TIMESTAMP,
    ingestion_date          DATE        NOT NULL
        COMMENT 'Partition key.'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'comment' = 'Gold: Fully enriched CVE view. NVD + EPSS + CISA + org exposure. Chatbot and analyst lookup.'
);
