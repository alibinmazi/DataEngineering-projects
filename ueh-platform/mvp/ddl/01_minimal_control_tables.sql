-- =============================================================================
-- UEH MVP: Minimal Control Tables (3 Tables Only)
-- =============================================================================
-- This is the MINIMUM needed to run the first NVD pipeline.
-- Run order: 1. Create database → 2. This file → 3. Seed data
--
-- Tables:
--   1. adapter_config  → What to ingest and how
--   2. adapter_state   → Where to resume (watermark)
--   3. batch_registry  → Track each batch lifecycle
--
-- Execution:
--   spark-sql -f mvp/ddl/01_minimal_control_tables.sql
-- =============================================================================

-- Create database (if not exists)
CREATE DATABASE IF NOT EXISTS ueh_dev_control
    LOCATION '/warehouse/dev/control';

USE ueh_dev_control;

-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE 1: ADAPTER CONFIG
-- ─────────────────────────────────────────────────────────────────────────────
-- "What do I ingest and how?"
-- NiFi reads this to know: URL, auth, pagination settings
-- Airflow reads this to know: schedule, SLA, active status
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_adapter_config (
    adapter_instance_id     STRING      NOT NULL    COMMENT 'Primary key. Example: nvd_public_01',
    adapter_name            STRING      NOT NULL    COMMENT 'Source name: nvd, epss, tenable',
    adapter_type            STRING      NOT NULL    COMMENT 'Category: vulnerability_intel, scanner, asset_inventory',
    base_url                STRING      NOT NULL    COMMENT 'API base URL',
    auth_method             STRING      NOT NULL    COMMENT 'none, api_key, oauth2',
    auth_secret_ref         STRING                  COMMENT 'Vault reference: vault://secrets/ueh/dev/nvd_api_key',
    ingestion_mode          STRING      NOT NULL    COMMENT 'INCREMENTAL or FULL',
    schedule_cron           STRING                  COMMENT 'Cron expression: 0 3 * * *',
    page_size               INT                     COMMENT 'API results per page',
    rate_limit_rps          INT                     COMMENT 'Max requests per second',
    sla_minutes             INT                     COMMENT 'Max allowed duration',
    is_active               BOOLEAN     NOT NULL    COMMENT 'TRUE = run on schedule',
    path_template           STRING      NOT NULL    COMMENT 'HDFS path template with ${placeholders}',
    created_at              TIMESTAMP   NOT NULL,
    updated_at              TIMESTAMP   NOT NULL
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE 2: ADAPTER STATE
-- ─────────────────────────────────────────────────────────────────────────────
-- "Where did I stop last time?"
-- NiFi reads watermark_value before calling API
-- NiFi updates watermark_value after successful ingestion
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_adapter_state (
    adapter_instance_id     STRING      NOT NULL    COMMENT 'FK to adapter_config',
    watermark_value         STRING                  COMMENT 'Last sync point (timestamp, offset, token)',
    watermark_type          STRING                  COMMENT 'iso_datetime, unix_timestamp, page_token',
    last_successful_run     TIMESTAMP               COMMENT 'When last successful run completed',
    records_last_pulled     BIGINT      DEFAULT 0   COMMENT 'Records in last successful run',
    consecutive_failures    INT         DEFAULT 0   COMMENT 'Failure streak (resets on success)',
    state_status            STRING      NOT NULL    COMMENT 'NEW, HEALTHY, FAILING',
    last_updated            TIMESTAMP   NOT NULL
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');


-- ─────────────────────────────────────────────────────────────────────────────
-- TABLE 3: BATCH REGISTRY
-- ─────────────────────────────────────────────────────────────────────────────
-- "What happened in each run?"
-- NiFi writes: status = RAW_COMPLETE (after writing to HDFS)
-- Spark writes: status = BRONZE_COMPLETE (after loading to Iceberg)
-- This is the COUPLING mechanism between DAG 1 and DAG 2
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_batch_registry (
    batch_id                STRING      NOT NULL    COMMENT 'Unique batch ID. Pattern: batch_{yyyyMMddHHmmss}_{adapter}',
    adapter_instance_id     STRING      NOT NULL    COMMENT 'FK to adapter_config',
    ingestion_date          DATE        NOT NULL    COMMENT 'Logical date (partition key)',
    load_type               STRING      NOT NULL    COMMENT 'INCREMENTAL, FULL_LOAD, REPLAY',
    status                  STRING      NOT NULL    COMMENT 'RAW_COMPLETE, BRONZE_COMPLETE, FAILED',
    records_ingested        BIGINT                  COMMENT 'Total records in batch',
    chunks_written          INT                     COMMENT 'Number of chunk files',
    bronze_path             STRING                  COMMENT 'HDFS path to raw data',
    watermark_start         STRING                  COMMENT 'Watermark at batch start',
    watermark_end           STRING                  COMMENT 'New watermark after batch',
    started_at              TIMESTAMP               COMMENT 'Batch start time',
    completed_at            TIMESTAMP               COMMENT 'Batch completion time',
    failure_reason          STRING                  COMMENT 'Error message if FAILED',
    trigger_type            STRING                  COMMENT 'SCHEDULED, MANUAL, REPLAY',
    created_at              TIMESTAMP   NOT NULL,
    updated_at              TIMESTAMP   NOT NULL
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES ('format-version' = '2');


-- ─────────────────────────────────────────────────────────────────────────────
-- Verify
-- ─────────────────────────────────────────────────────────────────────────────
SHOW TABLES IN ueh_dev_control;
