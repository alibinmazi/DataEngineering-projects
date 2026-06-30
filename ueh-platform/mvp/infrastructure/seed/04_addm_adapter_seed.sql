-- =============================================================================
-- UEH Seed: BMC ADDM Adapter Configuration
-- =============================================================================
-- Registers BMC ADDM asset discovery adapter.
-- ADDM uses standard REST API with offset-based pagination.
--
-- API Flow:
--   GET /data/hosts?limit=500&offset=0&modified_since={watermark}
--   Paginate until total_count reached
--
-- Watermark: modified_since (ISO datetime) for incremental discovery
-- Typically single instance per org (unlike scanners)
--
-- Run AFTER: infrastructure/control_tables
-- Database: t01_ueh_dev_ctl
-- =============================================================================

USE t01_ueh_dev_ctl;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Register ADDM Adapter Instance
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO t01_ueh_ctl_adapter_config VALUES (
    'default_org',                                                  -- org_id
    'addm_prod_01',                                                 -- adapter_instance_id
    'BMC_ADDM',                                                     -- source_system
    'REST_API',                                                     -- adapter_type
    'DEV',                                                          -- environment
    'https://addm.internal.company.com/api/v1',                    -- base_url
    'BASIC_AUTH',                                                    -- auth_method
    'vault://secrets/ueh/dev/addm_prod_01',                        -- auth_secret_ref
    'INCREMENTAL',                                                  -- ingestion_mode
    '0 2 * * *',                                                    -- schedule_cron (daily 2 AM)
    true,                                                           -- schedule_enabled
    30,                                                             -- sla_minutes
    '{"type":"offset","page_size":500,"page_size_param":"limit","offset_param":"offset","total_results_path":"$.total_count"}',  -- pagination_config_json
    '{"timeout_sec":120,"max_retries":3,"rate_limit_rps":10,"endpoints":["/data/hosts","/data/network_devices"]}',  -- runtime_config_json
    'asset_inventory/bmc_addm/raw/ingestion_date=${ingestion_date}/batch_id=${batch_id}',  -- path_template
    true,                                                           -- is_active
    current_timestamp(),                                            -- created_at
    current_timestamp()                                             -- updated_at
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Initialize ADDM Adapter State
-- ─────────────────────────────────────────────────────────────────────────────
-- ADDM uses ISO datetime watermark for modified_since parameter

INSERT INTO t01_ueh_ctl_adapter_state VALUES (
    'default_org',                                                  -- org_id
    'addm_prod_01',                                                 -- adapter_instance_id
    '{"modified_since":"2024-01-01T00:00:00.000","watermark_type":"iso_datetime","watermark_field":"modified_since"}',  -- watermark_state_json
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
WHERE adapter_instance_id = 'addm_prod_01';

SELECT adapter_instance_id, watermark_state_json, state_status
FROM t01_ueh_ctl_adapter_state
WHERE adapter_instance_id = 'addm_prod_01';


-- =============================================================================
-- ALL 3 ADAPTERS REGISTERED (verify):
-- =============================================================================

SELECT adapter_instance_id, source_system, schedule_cron, is_active
FROM t01_ueh_ctl_adapter_config
WHERE is_active = TRUE
ORDER BY source_system;

-- Expected:
-- addm_prod_01         | BMC_ADDM  | 0 2 * * *    | true
-- nvd_prod_01          | NVD       | 0 3 * * *    | true
-- tenable_prod_us_01   | TENABLE   | 0 */4 * * *  | true
