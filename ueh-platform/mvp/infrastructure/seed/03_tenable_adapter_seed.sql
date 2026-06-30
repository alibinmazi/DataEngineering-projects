-- =============================================================================
-- UEH Seed: Tenable Adapter Configuration
-- =============================================================================
-- Registers Tenable vulnerability scanner adapter (US instance).
-- Tenable uses async export API with chunk-based downloads.
--
-- API Flow:
--   1. POST /vulns/export (request export) → returns export_uuid
--   2. GET /vulns/export/{uuid}/status (poll) → wait for FINISHED
--   3. GET /vulns/export/{uuid}/chunks/{id} (download each chunk)
--
-- Watermark: last_found (unix timestamp) for incremental exports
-- Multiple instances supported (US, EU, APAC)
--
-- Run AFTER: infrastructure/control_tables (adapter_config + adapter_state)
-- Database: t01_ueh_dev_ctl
-- =============================================================================

USE t01_ueh_dev_ctl;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Register Tenable Adapter Instance (US Production)
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO t01_ueh_ctl_adapter_config VALUES (
    'default_org',                                                  -- org_id
    'tenable_prod_us_01',                                           -- adapter_instance_id
    'TENABLE',                                                      -- source_system
    'REST_API',                                                     -- adapter_type
    'DEV',                                                          -- environment
    'https://cloud.tenable.com',                                    -- base_url
    'API_KEY',                                                      -- auth_method
    'vault://secrets/ueh/dev/tenable_prod_us_01',                  -- auth_secret_ref
    'INCREMENTAL',                                                  -- ingestion_mode
    '0 */4 * * *',                                                  -- schedule_cron (every 4 hours)
    true,                                                           -- schedule_enabled
    45,                                                             -- sla_minutes
    '{"type":"export_job","export_endpoint":"/vulns/export","status_endpoint":"/vulns/export/{export_uuid}/status","download_endpoint":"/vulns/export/{export_uuid}/chunks/{chunk_id}","poll_interval_sec":30,"max_poll_attempts":120}',  -- pagination_config_json
    '{"timeout_sec":300,"max_retries":3,"rate_limit_rps":3,"chunk_download_parallel":2,"filters":{"severity":["low","medium","high","critical"],"state":["open","reopened"]}}',  -- runtime_config_json
    'scanners/tenable/raw/org_id=${org_id}/adapter_instance_id=${adapter_instance_id}/ingestion_date=${ingestion_date}/batch_id=${batch_id}',  -- path_template
    true,                                                           -- is_active
    current_timestamp(),                                            -- created_at
    current_timestamp()                                             -- updated_at
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Initialize Tenable Adapter State
-- ─────────────────────────────────────────────────────────────────────────────
-- Tenable uses unix timestamp for incremental watermark (last_found filter)
-- Also tracks export_uuid for resumability

INSERT INTO t01_ueh_ctl_adapter_state VALUES (
    'default_org',                                                  -- org_id
    'tenable_prod_us_01',                                           -- adapter_instance_id
    '{"last_found":1704067200,"watermark_type":"unix_timestamp","export_uuid":null,"last_export_status":null}',  -- watermark_state_json (2024-01-01 00:00:00 UTC)
    NULL,                                                           -- last_batch_id
    NULL,                                                           -- last_successful_run
    0,                                                              -- records_last_pulled
    0,                                                              -- consecutive_failures
    'NEW',                                                          -- state_status
    NULL,                                                           -- last_failure_reason
    current_timestamp()                                             -- updated_at
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Verify
-- ─────────────────────────────────────────────────────────────────────────────

SELECT adapter_instance_id, source_system, schedule_cron, sla_minutes, is_active
FROM t01_ueh_ctl_adapter_config
WHERE adapter_instance_id = 'tenable_prod_us_01';

SELECT adapter_instance_id, watermark_state_json, state_status
FROM t01_ueh_ctl_adapter_state
WHERE adapter_instance_id = 'tenable_prod_us_01';
