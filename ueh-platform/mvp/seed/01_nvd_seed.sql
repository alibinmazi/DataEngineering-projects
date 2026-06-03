-- =============================================================================
-- UEH MVP: Seed NVD Adapter Configuration
-- =============================================================================
-- Run AFTER: mvp/ddl/01_minimal_control_tables.sql
--
-- This registers NVD as the first adapter and sets the initial watermark.
-- After this, NiFi can read config + state to start ingesting.
--
-- Execution:
--   spark-sql -f mvp/seed/01_nvd_seed.sql
-- =============================================================================

USE ueh_dev_control;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Register NVD Adapter
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO t01_ueh_ctl_adapter_config VALUES (
    'nvd_public_01',                                                -- adapter_instance_id
    'nvd',                                                          -- adapter_name
    'vulnerability_intel',                                          -- adapter_type
    'https://services.nvd.nist.gov/rest/json/cves/2.0',           -- base_url
    'api_key',                                                      -- auth_method
    'vault://secrets/ueh/dev/nvd_api_key',                         -- auth_secret_ref
    'INCREMENTAL',                                                  -- ingestion_mode
    '0 3 * * *',                                                    -- schedule_cron (daily 3 AM)
    2000,                                                           -- page_size (NVD max)
    5,                                                              -- rate_limit_rps
    60,                                                             -- sla_minutes
    TRUE,                                                           -- is_active
    'vulnerability_intel/nvd/raw/ingestion_date=${ingestion_date}/batch_id=${batch_id}',
    CURRENT_TIMESTAMP(),                                            -- created_at
    CURRENT_TIMESTAMP()                                             -- updated_at
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Initialize Adapter State (Starting Watermark)
-- ─────────────────────────────────────────────────────────────────────────────
-- We start from 2024-01-01. First run will pull all CVEs modified since then.
-- After first run, watermark advances to "now" and subsequent runs are small.

INSERT INTO t01_ueh_ctl_adapter_state VALUES (
    'nvd_public_01',                    -- adapter_instance_id
    '2024-01-01T00:00:00.000',          -- watermark_value (start date)
    'iso_datetime',                     -- watermark_type
    NULL,                               -- last_successful_run (first time)
    0,                                  -- records_last_pulled
    0,                                  -- consecutive_failures
    'NEW',                              -- state_status
    CURRENT_TIMESTAMP()                 -- last_updated
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Verify
-- ─────────────────────────────────────────────────────────────────────────────

SELECT adapter_instance_id, adapter_name, base_url, is_active
FROM t01_ueh_ctl_adapter_config;

SELECT adapter_instance_id, watermark_value, state_status
FROM t01_ueh_ctl_adapter_state;
