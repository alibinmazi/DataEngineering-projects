-- =============================================================================
-- UEH Platform: Seed Data - NVD Adapter Configuration
-- =============================================================================
-- Purpose: Register the NVD adapter instance and initialize its state
-- Run AFTER: ddl/02_control_tables.sql
--
-- NVD API Details:
--   Endpoint: https://services.nvd.nist.gov/rest/json/cves/2.0
--   Auth: API Key (free registration at https://nvd.nist.gov/developers/request-an-api-key)
--   Pagination: offset-based (startIndex + resultsPerPage, max 2000)
--   Rate Limit: 50 req/30s with API key, 5 req/30s without
--   Incremental: lastModStartDate / lastModEndDate parameters
--
-- Execution: spark-sql -f seed/01_nvd_adapter_seed.sql
-- =============================================================================

USE ueh_dev_control;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Register NVD Adapter Instance
-- ─────────────────────────────────────────────────────────────────────────────
-- NVD is a global public feed — no org_id, single instance globally

INSERT INTO t01_ueh_ctl_adapter_config VALUES (
    -- Identity
    'nvd_public_01',                                                    -- adapter_instance_id
    'nvd',                                                              -- adapter_name
    'vulnerability_intel',                                              -- adapter_type
    NULL,                                                               -- org_id (global feed, no org context)
    
    -- Connection
    'us-east',                                                          -- region (NVD hosted in US)
    'https://services.nvd.nist.gov/rest/json/cves/2.0',               -- base_url
    'api_key',                                                          -- auth_method
    'vault://secrets/ueh/dev/nvd_api_key',                             -- auth_secret_ref
    
    -- Ingestion Behavior
    'INCREMENTAL',                                                      -- ingestion_mode (supports lastModStartDate)
    'INCREMENTAL',                                                      -- default_load_type
    '0 3 * * *',                                                        -- schedule_cron (daily at 3 AM)
    2000,                                                               -- chunk_size (records per chunk file)
    2000,                                                               -- page_size (NVD max resultsPerPage)
    5,                                                                  -- rate_limit_rps (conservative: 5 req/sec)
    120,                                                                -- request_timeout_sec
    3,                                                                  -- max_retries
    
    -- Path Configuration
    'vulnerability_intel/nvd/raw/ingestion_date=${ingestion_date}/batch_id=${batch_id}',  -- path_template
    
    -- Operational
    60,                                                                 -- sla_minutes (should complete within 1 hour)
    TRUE,                                                               -- is_active
    5,                                                                  -- priority (medium)
    CURRENT_DATE(),                                                     -- onboarded_date
    NULL,                                                               -- decommissioned_date
    'vuln-intel-team',                                                  -- owner_team
    'NVD public CVE feed. Free API key required. Max 2000 results per page. Incremental via lastModStartDate.',  -- notes
    
    -- Audit
    CURRENT_TIMESTAMP(),                                                -- created_at
    CURRENT_TIMESTAMP(),                                                -- updated_at
    'ueh-admin',                                                        -- created_by
    'ueh-admin'                                                         -- updated_by
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Initialize Adapter State
-- ─────────────────────────────────────────────────────────────────────────────
-- Start watermark: 2024-01-01 to capture recent CVEs without full historical load
-- For a complete backfill, set watermark_value to an earlier date

INSERT INTO t01_ueh_ctl_adapter_state VALUES (
    -- Identity
    'nvd_public_01',                            -- adapter_instance_id
    
    -- Watermark / Cursor
    '2024-01-01T00:00:00.000',                  -- watermark_value (ISO 8601 format for NVD API)
    'iso_datetime',                             -- watermark_type
    'lastModStartDate',                         -- watermark_field (NVD API parameter name)
    
    -- Last Run Info
    NULL,                                       -- last_successful_run (no runs yet)
    NULL,                                       -- last_attempted_run
    0,                                          -- records_last_pulled
    
    -- Health
    0,                                          -- consecutive_failures
    'NEW',                                      -- state_status (brand new adapter)
    NULL,                                       -- last_failure_reason
    
    -- Circuit Breaker
    FALSE,                                      -- circuit_breaker_open
    NULL,                                       -- circuit_breaker_opened_at
    
    -- Audit
    CURRENT_TIMESTAMP()                         -- last_updated
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Verification Queries
-- ─────────────────────────────────────────────────────────────────────────────

-- Verify adapter config registered
SELECT 
    adapter_instance_id,
    adapter_name,
    adapter_type,
    base_url,
    ingestion_mode,
    is_active,
    schedule_cron
FROM t01_ueh_ctl_adapter_config
WHERE adapter_instance_id = 'nvd_public_01';

-- Verify adapter state initialized
SELECT 
    adapter_instance_id,
    watermark_value,
    watermark_type,
    state_status,
    consecutive_failures
FROM t01_ueh_ctl_adapter_state
WHERE adapter_instance_id = 'nvd_public_01';


-- =============================================================================
-- NOTES FOR FIRST RUN BEHAVIOR:
-- =============================================================================
-- 
-- The first ingestion run will:
--   1. Read watermark_value = '2024-01-01T00:00:00.000'
--   2. Call NVD API: ?lastModStartDate=2024-01-01T00:00:00.000&lastModEndDate={now}
--   3. This will return ALL CVEs modified since 2024-01-01 (could be 20,000+ records)
--   4. NiFi will paginate through all pages (startIndex=0, 2000, 4000, ...)
--   5. Records are chunked into files of 2000 records each
--   6. After completion, watermark advances to {now}
--   7. Next day's run only gets CVEs modified since last watermark
--
-- FIRST RUN LOAD TYPE:
--   Even though ingestion_mode = INCREMENTAL, the first run is effectively a
--   "large incremental" because the watermark starts at 2024-01-01.
--   batch_registry.load_type will be recorded as 'INCREMENTAL' (not FULL_LOAD)
--   because the mechanism IS incremental — just with a wide date range.
--
--   Set watermark_value to '1999-01-01T00:00:00.000' for true full historical load.
--
-- =============================================================================
