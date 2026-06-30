-- =============================================================================
-- UEH Gold Table: Exposure Summary
-- =============================================================================
-- Purpose: Enriched vulnerability exposure view combining:
--   - Scanner findings (slv_vulnerability_findings)
--   - CVE intelligence (slv_vulnerability_intel) 
--   - Asset context (slv_assets)
--
-- One row per (finding + CVE enrichment + asset context)
-- This is the PRIMARY table for dashboards, prioritization, and reporting.
--
-- Fed by: JOIN of all 3 Silver tables
-- Strategy: OVERWRITE partition (rebuild daily)
-- =============================================================================

USE t01_ueh_dev_gld;

CREATE TABLE IF NOT EXISTS t01_ueh_gld_exposure_summary (

    -- ─── Finding Identity ────────────────────────────────────────────
    finding_id              STRING      NOT NULL
        COMMENT 'FK → slv_vulnerability_findings.',
    source_system           STRING      NOT NULL
        COMMENT 'Scanner source: TENABLE, SYSDIG, QUALYS.',

    -- ─── Vulnerability Context (from slv_vulnerability_intel) ────────
    cve_id                  STRING
        COMMENT 'CVE identifier. NULL for non-CVE findings.',
    vulnerability_name      STRING
        COMMENT 'Human-readable name.',
    description             STRING
        COMMENT 'CVE description.',
    severity                STRING
        COMMENT 'Normalized: CRITICAL, HIGH, MEDIUM, LOW, INFORMATIONAL.',
    cvss_base_score         DOUBLE
        COMMENT 'CVSS score (0.0-10.0).',
    cvss_version            STRING,

    -- ─── Threat Intelligence (enrichment from EPSS + CISA) ───────────
    epss_score              DOUBLE
        COMMENT 'EPSS exploit probability (0.0-1.0). Higher = more likely exploited.',
    epss_percentile         DOUBLE
        COMMENT 'EPSS percentile ranking.',
    is_in_kev               BOOLEAN
        COMMENT 'In CISA Known Exploited Vulnerabilities catalog.',
    kev_due_date            DATE
        COMMENT 'CISA remediation deadline.',
    is_actively_exploited   BOOLEAN
        COMMENT 'Active exploitation confirmed.',

    -- ─── Asset Context (from slv_assets) ─────────────────────────────
    asset_id                STRING
        COMMENT 'FK → slv_assets.',
    asset_ip                STRING,
    asset_hostname          STRING,
    asset_fqdn              STRING,
    asset_os                STRING,
    asset_type              STRING
        COMMENT 'COMPUTE, CONTAINER, NETWORK, DATABASE.',
    asset_criticality       STRING
        COMMENT 'Business criticality: CRITICAL, HIGH, MEDIUM, LOW.',
    asset_environment       STRING
        COMMENT 'PRODUCTION, STAGING, DEVELOPMENT.',
    asset_business_unit     STRING,
    asset_owner             STRING,

    -- ─── Risk Scoring (calculated) ───────────────────────────────────
    risk_score              DOUBLE
        COMMENT 'UEH composite risk score (0-100). Combines CVSS + EPSS + KEV + asset criticality.',
    risk_category           STRING
        COMMENT 'CRITICAL_RISK, HIGH_RISK, MEDIUM_RISK, LOW_RISK.',
    priority_rank           INT
        COMMENT 'Priority rank within org (1 = highest priority to fix).',

    -- ─── Exposure Timing ─────────────────────────────────────────────
    first_seen              TIMESTAMP
        COMMENT 'When vulnerability first detected on this asset.',
    last_seen               TIMESTAMP
        COMMENT 'When last confirmed.',
    days_exposed            INT
        COMMENT 'Days between first_seen and now (or fixed_at).',
    sla_status              STRING
        COMMENT 'WITHIN_SLA, APPROACHING_SLA, SLA_BREACHED.',

    -- ─── Finding Status ──────────────────────────────────────────────
    status                  STRING
        COMMENT 'OPEN, FIXED, REOPENED, ACCEPTED.',
    fixed_at                TIMESTAMP,
    days_to_remediate       INT
        COMMENT 'Days from first_seen to fixed_at (NULL if open).',

    -- ─── Source Detail ───────────────────────────────────────────────
    source_risk_score       DOUBLE
        COMMENT 'Vendor risk score (Tenable VPR, Qualys TruRisk).',
    solution                STRING
        COMMENT 'Remediation recommendation.',
    output                  STRING
        COMMENT 'Scanner proof/evidence (truncated).',

    -- ─── UEH Metadata ────────────────────────────────────────────────
    adapter_instance_id     STRING,
    batch_id                STRING,
    gold_computed_at        TIMESTAMP
        COMMENT 'When this Gold record was computed.',
    ingestion_date          DATE        NOT NULL
        COMMENT 'Partition key (date of Gold computation).'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'comment' = 'Gold: Enriched exposure summary. JOIN of findings + intel + assets. Primary analytics table.'
);
