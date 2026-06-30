-- =============================================================================
-- MIGRATION 001: Add dq_status to batch_registry
-- =============================================================================
-- Purpose: Enable DQ-gated orchestration. DAGs will check both:
--   batch_status (pipeline lifecycle) AND dq_status (quality gate)
--
-- Rules:
--   - NEVER drop/recreate batch_registry
--   - Use ALTER TABLE ADD COLUMN only
--   - Backward compatible (existing rows get NULL → treated as 'NOT_CHECKED')
--
-- Execution:
--   spark-sql -f infrastructure/migrations/001_add_dq_status_to_batch_registry.sql
--
-- Validation:
--   DESCRIBE t01_ueh_dev_ctl.t01_ueh_ctl_batch_registry;
--   → should show dq_status column
-- =============================================================================

USE t01_ueh_dev_ctl;

-- Add dq_status column (quality gate for downstream DAGs)
ALTER TABLE t01_ueh_ctl_batch_registry
ADD COLUMN dq_status STRING
COMMENT 'Data quality gate: PASSED | WARNING | FAILED | NOT_CHECKED. Downstream DAGs proceed only when PASSED or WARNING.';

-- Add dq_details_json column (stores DQ check results)
ALTER TABLE t01_ueh_ctl_batch_registry
ADD COLUMN dq_details_json STRING
COMMENT 'JSON with DQ check results: {"null_count":0,"invalid_format":2,"completeness":0.95}';


-- =============================================================================
-- USAGE IN ORCHESTRATION:
-- =============================================================================
-- PythonSensor gate logic:
--   PROCEED when: batch_status = 'BRONZE_COMPLETE' AND dq_status IN ('PASSED', 'WARNING')
--   BLOCK when:   dq_status = 'FAILED'
--   WAIT when:    dq_status IS NULL or 'NOT_CHECKED' (DQ hasn't run yet)
--
-- WHO WRITES dq_status:
--   Bronze Spark job → writes dq_status after Bronze DQ checks
--   Silver Stage 1 Spark job → writes dq_status after staging DQ checks
--   Silver Stage 2 (Canonical) → writes dq_status after canonical DQ
--
-- VALUES:
--   'PASSED'      → All DQ checks passed. Proceed.
--   'WARNING'     → Minor issues (e.g., <5% null rate). Proceed with caution.
--   'FAILED'      → Critical issues. BLOCK downstream processing.
--   'NOT_CHECKED' → DQ hasn't been evaluated yet.
--   NULL          → Legacy rows (treat as 'NOT_CHECKED' for backward compat)
-- =============================================================================
