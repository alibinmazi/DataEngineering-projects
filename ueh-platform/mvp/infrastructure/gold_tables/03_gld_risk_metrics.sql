-- =============================================================================
-- UEH Gold Table: Risk Metrics (Aggregated)
-- =============================================================================
-- Purpose: Daily aggregated risk metrics per dimension
-- Used by: Executive dashboards, trend analysis, SLA reporting
-- Strategy: OVERWRITE partition daily (rebuild from exposure_summary)
--
-- Provides answers to:
--   "How many CRITICAL vulns are open across production?"
--   "What's our mean-time-to-remediate this month?"
--   "Which business unit has the worst exposure?"
-- =============================================================================

USE t01_ueh_dev_gld;

CREATE TABLE IF NOT EXISTS t01_ueh_gld_risk_metrics (

    -- ─── Dimension ───────────────────────────────────────────────────
    metric_date             DATE        NOT NULL
        COMMENT 'Date this metric was computed. Partition key.',
    dimension_type          STRING      NOT NULL
        COMMENT 'What this row aggregates by: OVERALL, BY_SEVERITY, BY_BUSINESS_UNIT, BY_ASSET_TYPE, BY_ENVIRONMENT, BY_SOURCE.',
    dimension_value         STRING
        COMMENT 'Value of dimension. E.g., CRITICAL, Finance, PRODUCTION, TENABLE.',

    -- ─── Counts ──────────────────────────────────────────────────────
    total_findings          BIGINT
        COMMENT 'Total findings matching this dimension.',
    open_findings           BIGINT
        COMMENT 'Currently open findings.',
    fixed_findings          BIGINT
        COMMENT 'Fixed findings.',
    critical_open           BIGINT
        COMMENT 'Open CRITICAL findings.',
    high_open               BIGINT
        COMMENT 'Open HIGH findings.',
    kev_open                BIGINT
        COMMENT 'Open findings in CISA KEV.',
    exploitable_open        BIGINT
        COMMENT 'Open findings with EPSS > 0.7.',

    -- ─── Risk Scores ─────────────────────────────────────────────────
    avg_risk_score          DOUBLE
        COMMENT 'Average UEH risk score.',
    max_risk_score          DOUBLE
        COMMENT 'Maximum risk score.',
    avg_cvss                DOUBLE
        COMMENT 'Average CVSS base score.',
    avg_epss                DOUBLE
        COMMENT 'Average EPSS score.',

    -- ─── Timing Metrics ──────────────────────────────────────────────
    avg_days_exposed        DOUBLE
        COMMENT 'Average days findings have been open.',
    max_days_exposed        INT
        COMMENT 'Longest open finding (days).',
    avg_days_to_remediate   DOUBLE
        COMMENT 'Average days to fix (for fixed findings).',
    sla_breach_count        BIGINT
        COMMENT 'Findings that breached remediation SLA.',

    -- ─── Asset Metrics ───────────────────────────────────────────────
    unique_assets_affected  BIGINT
        COMMENT 'Distinct assets with open findings.',
    critical_assets_affected BIGINT
        COMMENT 'Critical-criticality assets with open findings.',

    -- ─── Trend ───────────────────────────────────────────────────────
    new_findings_today      BIGINT
        COMMENT 'New findings discovered today.',
    fixed_today             BIGINT
        COMMENT 'Findings fixed today.',
    reopened_today          BIGINT
        COMMENT 'Findings reopened today.',
    net_change              BIGINT
        COMMENT 'new - fixed (positive = growing exposure).',

    -- ─── UEH Metadata ────────────────────────────────────────────────
    computed_at             TIMESTAMP
        COMMENT 'When this metric was computed.'
)
USING iceberg
PARTITIONED BY (metric_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'comment' = 'Gold: Daily aggregated risk metrics. Executive dashboards and trend reporting.'
);
