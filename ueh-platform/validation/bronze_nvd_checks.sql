-- =============================================================================
-- UEH Platform: Validation Queries - NVD Bronze Layer
-- =============================================================================
-- Purpose: End-to-end validation after NVD Bronze pipeline execution.
--          Run these queries to confirm data integrity across all layers.
--
-- Usage:   spark-sql -f validation/bronze_nvd_checks.sql
--          OR run individually in Spark SQL / Hue / DBeaver
--
-- Sections:
--   1. Control Table Health
--   2. Bronze Data Integrity
--   3. Data Quality Flags
--   4. Operational Metrics
--   5. Failure & Dead Letter Monitoring
--   6. End-to-End Pipeline Status
-- =============================================================================

-- NOTE: Replace 'dev' with target environment or use variable substitution
-- SET ueh_env = 'dev';

USE ueh_dev_control;


-- =============================================================================
-- SECTION 1: CONTROL TABLE HEALTH
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1.1 Adapter Configuration Summary
-- Verify NVD adapter is registered and active
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    adapter_instance_id,
    adapter_name,
    adapter_type,
    ingestion_mode,
    is_active,
    schedule_cron,
    sla_minutes,
    onboarded_date
FROM t01_ueh_ctl_adapter_config
WHERE adapter_name = 'nvd';

-- Expected: 1 row, is_active = TRUE


-- ─────────────────────────────────────────────────────────────────────────────
-- 1.2 Adapter State Health
-- Check watermark progression and health status
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    adapter_instance_id,
    state_status,
    watermark_value,
    watermark_type,
    last_successful_run,
    records_last_pulled,
    consecutive_failures,
    circuit_breaker_open,
    last_updated
FROM t01_ueh_ctl_adapter_state
WHERE adapter_instance_id = 'nvd_public_01';

-- Expected: state_status = 'HEALTHY', circuit_breaker_open = FALSE
-- After first run: watermark_value should advance past initial seed value


-- ─────────────────────────────────────────────────────────────────────────────
-- 1.3 Watermark Progression Over Time
-- Verify watermark advances with each run (no stale state)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    batch_id,
    ingestion_date,
    watermark_start,
    watermark_end,
    records_ingested,
    status
FROM t01_ueh_ctl_batch_registry
WHERE adapter_instance_id = 'nvd_public_01'
ORDER BY started_at DESC
LIMIT 10;

-- Expected: watermark_end of each batch = watermark_start of next batch
-- This confirms continuous incremental coverage with no gaps


-- =============================================================================
-- SECTION 2: BRONZE DATA INTEGRITY
-- =============================================================================

USE ueh_dev_bronze;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2.1 Overall Record Count by Ingestion Date
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    ingestion_date,
    batch_id,
    load_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT source_record_id) as unique_cves
FROM t01_ueh_brz_nvd_vulnerabilities
GROUP BY ingestion_date, batch_id, load_type
ORDER BY ingestion_date DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.2 Duplicate Detection Within Batch
-- Same CVE should NOT appear twice in the same batch
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    batch_id,
    source_record_id,
    COUNT(*) as occurrences
FROM t01_ueh_brz_nvd_vulnerabilities
GROUP BY batch_id, source_record_id
HAVING COUNT(*) > 1
LIMIT 20;

-- Expected: ZERO rows (no intra-batch duplicates)
-- NOTE: Cross-batch duplicates ARE expected for incremental (CVE modified again)


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.3 Payload JSON Completeness
-- Verify payload_json is non-null and contains expected structure markers
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    batch_id,
    COUNT(*) as total_records,
    SUM(CASE WHEN payload_json IS NULL THEN 1 ELSE 0 END) as null_payloads,
    SUM(CASE WHEN payload_json = '' THEN 1 ELSE 0 END) as empty_payloads,
    SUM(CASE WHEN payload_json NOT LIKE '%"cve"%' THEN 1 ELSE 0 END) as missing_cve_key,
    AVG(LENGTH(payload_json)) as avg_payload_size_bytes,
    MIN(LENGTH(payload_json)) as min_payload_size_bytes,
    MAX(LENGTH(payload_json)) as max_payload_size_bytes
FROM t01_ueh_brz_nvd_vulnerabilities
WHERE ingestion_date = CURRENT_DATE()
GROUP BY batch_id;

-- Expected: null_payloads = 0, empty_payloads = 0, missing_cve_key = 0
-- Typical avg_payload_size: 2000-10000 bytes per CVE record


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.4 Source Record ID Format Validation
-- CVE IDs should follow pattern: CVE-YYYY-NNNNN
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    batch_id,
    COUNT(*) as total_records,
    SUM(CASE WHEN source_record_id RLIKE '^CVE-[0-9]{4}-[0-9]{4,}$' THEN 1 ELSE 0 END) as valid_cve_ids,
    SUM(CASE WHEN source_record_id IS NULL THEN 1 ELSE 0 END) as null_ids,
    SUM(CASE WHEN source_record_id NOT RLIKE '^CVE-[0-9]{4}-[0-9]{4,}$' 
             AND source_record_id IS NOT NULL THEN 1 ELSE 0 END) as invalid_format_ids
FROM t01_ueh_brz_nvd_vulnerabilities
WHERE ingestion_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY batch_id;

-- Expected: valid_cve_ids = total_records, null_ids = 0, invalid_format_ids = 0


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.5 Sample Records (Spot Check)
-- View a few records to manually verify payload structure
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    batch_id,
    source_record_id,
    ingestion_timestamp,
    load_type,
    chunk_file,
    record_index_in_chunk,
    SUBSTR(payload_json, 1, 500) as payload_preview
FROM t01_ueh_brz_nvd_vulnerabilities
WHERE ingestion_date = CURRENT_DATE()
ORDER BY record_index_in_chunk ASC
LIMIT 5;


-- =============================================================================
-- SECTION 3: DATA QUALITY FLAGS
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 3.1 DQ Flag Summary by Batch
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    batch_id,
    ingestion_date,
    COUNT(*) as total_records,
    SUM(CASE WHEN dq_is_valid_json = TRUE THEN 1 ELSE 0 END) as valid_json_count,
    SUM(CASE WHEN dq_is_valid_json = FALSE THEN 1 ELSE 0 END) as invalid_json_count,
    SUM(CASE WHEN dq_has_record_id = TRUE THEN 1 ELSE 0 END) as has_record_id_count,
    SUM(CASE WHEN dq_has_record_id = FALSE THEN 1 ELSE 0 END) as missing_record_id_count,
    AVG(dq_payload_size_bytes) as avg_payload_bytes,
    MIN(dq_payload_size_bytes) as min_payload_bytes,
    MAX(dq_payload_size_bytes) as max_payload_bytes
FROM t01_ueh_brz_nvd_vulnerabilities
WHERE ingestion_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY batch_id, ingestion_date
ORDER BY ingestion_date DESC;

-- Expected: invalid_json_count = 0, missing_record_id_count = 0


-- ─────────────────────────────────────────────────────────────────────────────
-- 3.2 Records Failing DQ (if any)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    batch_id,
    source_record_id,
    dq_is_valid_json,
    dq_has_record_id,
    dq_payload_size_bytes,
    chunk_file,
    SUBSTR(payload_json, 1, 200) as payload_snippet
FROM t01_ueh_brz_nvd_vulnerabilities
WHERE (dq_is_valid_json = FALSE OR dq_has_record_id = FALSE)
  AND ingestion_date >= DATE_SUB(CURRENT_DATE(), 7)
LIMIT 20;

-- Expected: ZERO rows (all DQ flags should pass)


-- =============================================================================
-- SECTION 4: OPERATIONAL METRICS
-- =============================================================================

USE ueh_dev_control;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4.1 Batch Execution History (Last 14 Days)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    batch_id,
    ingestion_date,
    status,
    load_type,
    records_ingested,
    chunks_written,
    started_at,
    raw_completed_at,
    bronze_completed_at,
    -- Duration calculations
    TIMESTAMPDIFF(MINUTE, started_at, raw_completed_at) as raw_duration_min,
    TIMESTAMPDIFF(MINUTE, raw_completed_at, bronze_completed_at) as bronze_duration_min,
    TIMESTAMPDIFF(MINUTE, started_at, bronze_completed_at) as total_duration_min
FROM t01_ueh_ctl_batch_registry
WHERE adapter_instance_id = 'nvd_public_01'
  AND ingestion_date >= DATE_SUB(CURRENT_DATE(), 14)
ORDER BY ingestion_date DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4.2 SLA Compliance Check
-- Which batches exceeded SLA?
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    br.batch_id,
    br.ingestion_date,
    br.status,
    TIMESTAMPDIFF(MINUTE, br.started_at, COALESCE(br.bronze_completed_at, CURRENT_TIMESTAMP())) as actual_duration_min,
    ac.sla_minutes,
    CASE 
        WHEN TIMESTAMPDIFF(MINUTE, br.started_at, COALESCE(br.bronze_completed_at, CURRENT_TIMESTAMP())) > ac.sla_minutes 
        THEN 'SLA_BREACH'
        ELSE 'WITHIN_SLA'
    END as sla_status
FROM t01_ueh_ctl_batch_registry br
JOIN t01_ueh_ctl_adapter_config ac 
    ON br.adapter_instance_id = ac.adapter_instance_id
WHERE br.adapter_instance_id = 'nvd_public_01'
  AND br.ingestion_date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY br.ingestion_date DESC;

-- Expected: All rows = 'WITHIN_SLA'


-- ─────────────────────────────────────────────────────────────────────────────
-- 4.3 Daily Record Volume Trend
-- Spot anomalies in daily record counts
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    ingestion_date,
    SUM(records_ingested) as daily_records,
    COUNT(batch_id) as batches_per_day,
    AVG(records_ingested) as avg_records_per_batch
FROM t01_ueh_ctl_batch_registry
WHERE adapter_instance_id = 'nvd_public_01'
  AND status IN ('RAW_COMPLETE', 'BRONZE_COMPLETE', 'SILVER_COMPLETE', 'GOLD_COMPLETE')
  AND ingestion_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY ingestion_date
ORDER BY ingestion_date DESC;

-- Watch for: sudden drops (API issue?), sudden spikes (mass CVE update?)


-- =============================================================================
-- SECTION 5: FAILURE & DEAD LETTER MONITORING
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 5.1 Recent Failures
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    batch_id,
    ingestion_date,
    status,
    failure_reason,
    failure_stage,
    retry_count,
    started_at
FROM t01_ueh_ctl_batch_registry
WHERE adapter_instance_id = 'nvd_public_01'
  AND status = 'FAILED'
  AND ingestion_date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY started_at DESC;

-- Expected: ZERO rows (no failures in last 30 days)


-- ─────────────────────────────────────────────────────────────────────────────
-- 5.2 Dead Letter Registry
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    failure_id,
    batch_id,
    failure_timestamp,
    failure_stage,
    failure_category,
    failure_reason,
    resolution_status,
    dead_letter_path,
    records_affected
FROM t01_ueh_ctl_failed_ingestions
WHERE adapter_instance_id = 'nvd_public_01'
  AND resolution_status = 'PENDING'
ORDER BY failure_timestamp DESC;

-- Expected: ZERO rows (no unresolved failures)


-- ─────────────────────────────────────────────────────────────────────────────
-- 5.3 Circuit Breaker Status (All Adapters)
-- Quick health check across all registered adapters
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    ac.adapter_instance_id,
    ac.adapter_name,
    ac.is_active,
    ast.state_status,
    ast.consecutive_failures,
    ast.circuit_breaker_open,
    ast.last_successful_run,
    ast.last_failure_reason
FROM t01_ueh_ctl_adapter_config ac
LEFT JOIN t01_ueh_ctl_adapter_state ast
    ON ac.adapter_instance_id = ast.adapter_instance_id
ORDER BY ast.consecutive_failures DESC;


-- =============================================================================
-- SECTION 6: END-TO-END PIPELINE STATUS
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 6.1 Pipeline Status Dashboard (Current State)
-- Single view of where each batch is in the pipeline
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    ingestion_date,
    batch_id,
    status,
    records_ingested,
    CASE 
        WHEN status = 'RAW_COMPLETE'    THEN '██░░░░░░░░ 25%'
        WHEN status = 'BRONZE_COMPLETE' THEN '█████░░░░░ 50%'
        WHEN status = 'SILVER_COMPLETE' THEN '███████░░░ 75%'
        WHEN status = 'GOLD_COMPLETE'   THEN '██████████ 100%'
        WHEN status = 'FAILED'          THEN '❌ FAILED'
        ELSE '⏳ ' || status
    END as pipeline_progress,
    started_at,
    COALESCE(gold_completed_at, silver_completed_at, bronze_completed_at, raw_completed_at) as last_stage_completed
FROM t01_ueh_ctl_batch_registry
WHERE adapter_instance_id = 'nvd_public_01'
  AND ingestion_date >= DATE_SUB(CURRENT_DATE(), 7)
ORDER BY ingestion_date DESC, started_at DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6.2 Batch Registry vs Bronze Table Reconciliation
-- Ensure batch_registry.records_ingested matches actual Bronze table count
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    br.batch_id,
    br.ingestion_date,
    br.records_ingested as registry_count,
    brz.actual_count,
    (brz.actual_count - br.records_ingested) as count_difference,
    CASE 
        WHEN brz.actual_count = br.records_ingested THEN 'MATCH'
        WHEN brz.actual_count IS NULL THEN 'MISSING_FROM_BRONZE'
        ELSE 'MISMATCH'
    END as reconciliation_status
FROM t01_ueh_ctl_batch_registry br
LEFT JOIN (
    SELECT batch_id, COUNT(*) as actual_count
    FROM ueh_dev_bronze.t01_ueh_brz_nvd_vulnerabilities
    GROUP BY batch_id
) brz ON br.batch_id = brz.batch_id
WHERE br.adapter_instance_id = 'nvd_public_01'
  AND br.status = 'BRONZE_COMPLETE'
  AND br.ingestion_date >= DATE_SUB(CURRENT_DATE(), 14)
ORDER BY br.ingestion_date DESC;

-- Expected: All rows = 'MATCH'


-- ─────────────────────────────────────────────────────────────────────────────
-- 6.3 Replay Queue Status
-- Any pending replays?
-- ─────────────────────────────────────────────────────────────────────────────
SELECT 
    replay_id,
    original_batch_id,
    replay_from_stage,
    replay_reason,
    priority,
    replay_status,
    requested_at,
    requested_by
FROM t01_ueh_ctl_replay_queue
WHERE adapter_instance_id = 'nvd_public_01'
  AND replay_status IN ('PENDING', 'IN_PROGRESS')
ORDER BY priority ASC, requested_at ASC;

-- Expected: ZERO rows (no pending replays under normal operation)


-- =============================================================================
-- SUMMARY: Quick Health Check (Run Daily)
-- =============================================================================

-- Single query that gives overall platform health
SELECT 
    'ADAPTER_STATUS' as check_type,
    CONCAT(ast.state_status, ' (failures: ', ast.consecutive_failures, ')') as result
FROM t01_ueh_ctl_adapter_state ast
WHERE ast.adapter_instance_id = 'nvd_public_01'

UNION ALL

SELECT 
    'LATEST_BATCH' as check_type,
    CONCAT(br.batch_id, ' → ', br.status, ' (', br.records_ingested, ' records)') as result
FROM t01_ueh_ctl_batch_registry br
WHERE br.adapter_instance_id = 'nvd_public_01'
ORDER BY br.started_at DESC
LIMIT 1

UNION ALL

SELECT 
    'PENDING_FAILURES' as check_type,
    CAST(COUNT(*) AS STRING) as result
FROM t01_ueh_ctl_failed_ingestions
WHERE adapter_instance_id = 'nvd_public_01'
  AND resolution_status = 'PENDING'

UNION ALL

SELECT 
    'CIRCUIT_BREAKER' as check_type,
    CASE WHEN circuit_breaker_open THEN 'OPEN ⚠️' ELSE 'CLOSED ✓' END as result
FROM t01_ueh_ctl_adapter_state
WHERE adapter_instance_id = 'nvd_public_01';
