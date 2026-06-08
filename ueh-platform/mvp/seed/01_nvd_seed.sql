-- =============================================================================
-- UEH MVP: Seed NVD Adapter Configuration
-- =============================================================================
-- Database: t01_ueh_dev_ctl
-- Run AFTER: mvp/ddl/01_control_tables.sql
--
-- This registers NVD as the first adapter and sets the initial watermark.
-- After this, NiFi can read config + state to start ingesting.
--
-- Execution:
--   spark-sql -f mvp/seed/01_nvd_seed.sql
-- =============================================================================

USE t01_ueh_dev_ctl;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Register NVD Adapter in adapter_config
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO t01_ueh_ctl_adapter_config VALUES (
    'default_org',                                              -- org_id
    'nvd_prod_01',                                              -- adapter_instance_id
    'NVD',                                                      -- source_system
    'REST_API',                                                 -- adapter_type
    'DEV',                                                      -- environment
    'https://services.nvd.nist.gov/rest/json/cves/2.0',        -- base_url
    'API_KEY',                                                  -- auth_method
    'vault://secrets/ueh/dev/nvd_api_key',                     -- auth_secret_ref
    'INCREMENTAL',                                              -- ingestion_mode
    '0 3 * * *',                                               -- schedule_cron
    TRUE,                                                       -- schedule_enabled
    60,                                                         -- sla_minutes
    '{"type":"offset","page_size":2000,"param_name":"startIndex","results_param":"resultsPerPage"}',  -- pagination_config_json
    '{"timeout_sec":120,"max_retries":3,"rate_limit_rps":5}',  -- runtime_config_json
    'vulnerability_intel/nvd/raw/ingestion_date=${ingestion_date}/batch_id=${batch_id}',  -- path_template
    TRUE,                                                       -- is_active
    CURRENT_TIMESTAMP(),                                        -- created_at
    CURRENT_TIMESTAMP()                                         -- updated_at
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Initialize Adapter State (Starting Watermark)
-- ─────────────────────────────────────────────────────────────────────────────
-- watermark_state_json holds the cursor state for NVD:
--   lastModStartDate = where to resume from
--   watermark_type = how to interpret the value
--
-- First run will pull all CVEs modified since 2024-01-01.
-- After first run, watermark advances to "now" and subsequent runs are small.

INSERT INTO t01_ueh_ctl_adapter_state VALUES (
    'default_org',                                              -- org_id
    'nvd_prod_01',                                              -- adapter_instance_id
    '{"lastModStartDate":"2024-01-01T00:00:00.000","watermark_type":"iso_datetime"}',  -- watermark_state_json
    NULL,                                                       -- last_batch_id (no runs yet)
    NULL,                                                       -- last_successful_run
    0,                                                          -- records_last_pulled
    0,                                                          -- consecutive_failures
    'NEW',                                                      -- state_status
    NULL,                                                       -- last_failure_reason
    CURRENT_TIMESTAMP()                                         -- updated_at
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Verify
-- ─────────────────────────────────────────────────────────────────────────────

SELECT org_id, adapter_instance_id, source_system, base_url, is_active, schedule_enabled
FROM t01_ueh_ctl_adapter_config
WHERE adapter_instance_id = 'nvd_prod_01';

SELECT org_id, adapter_instance_id, watermark_state_json, state_status
FROM t01_ueh_ctl_adapter_state
WHERE adapter_instance_id = 'nvd_prod_01';


-- =============================================================================
-- FIRST RUN NOTES:
-- =============================================================================
-- 1. NiFi reads watermark_state_json → extracts lastModStartDate = 2024-01-01
-- 2. NiFi calls: ?lastModStartDate=2024-01-01T00:00:00.000&lastModEndDate={now}
-- 3. This returns ALL CVEs modified since 2024-01-01 (could be 20,000+)
-- 4. NiFi paginates (startIndex=0, 2000, 4000, ...)
-- 5. Chunks written to HDFS
-- 6. batch_registry → RAW_COMPLETE
-- 7. adapter_state → watermark advances to {now}
-- 8. Next day only gets ~50-200 modified CVEs (incremental)
--
-- FOR TESTING: Set watermark to yesterday for a small first run:
--   UPDATE t01_ueh_ctl_adapter_state
--   SET watermark_state_json = '{"lastModStartDate":"2026-06-07T00:00:00.000","watermark_type":"iso_datetime"}'
--   WHERE adapter_instance_id = 'nvd_prod_01' AND org_id = 'default_org';
-- =============================================================================
