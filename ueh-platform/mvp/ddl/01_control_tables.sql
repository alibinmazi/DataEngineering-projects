-- =====================================================================
-- UEH MVP: Control Tables DDL (Aligned with Implementation)
-- =====================================================================
-- Database: t01_ueh_dev_ctl
-- Tables:
--   1. t01_ueh_ctl_adapter_config  → What to ingest and how
--   2. t01_ueh_ctl_adapter_state   → Where to resume (watermark)
--   3. t01_ueh_ctl_batch_registry  → Track each batch lifecycle
--
-- Execution:
--   spark-sql -f mvp/ddl/01_control_tables.sql
--
-- Changes from review:
--   - adapter_state: REMOVED partition (table <100 rows, mutable status)
--   - batch_registry: ADDED bronze_path, ingestion_date, chunks_written
--   - batch_registry: ADDED failure_category, parent_batch_id
-- =====================================================================


-- =====================================================================
-- DATABASE
-- =====================================================================

CREATE DATABASE IF NOT EXISTS t01_ueh_dev_ctl
    COMMENT 'UEH DEV control tables database'
    LOCATION '/warehouse/dev/control';



-- =====================================================================
-- 1. ADAPTER CONFIGURATION TABLE
-- =====================================================================
-- "What do I ingest and how?"
-- NiFi reads: base_url, auth_secret_ref, pagination_config_json
-- Airflow reads: schedule_cron, sla_minutes, is_active, schedule_enabled
-- =====================================================================

CREATE TABLE IF NOT EXISTS t01_ueh_dev_ctl.t01_ueh_ctl_adapter_config (

    -- ---------------------------------------------------------------
    -- Identity
    -- ---------------------------------------------------------------
    org_id                  STRING      NOT NULL
        COMMENT 'Logical organisation/tenant identifier. Use default_org or org001 in v1.',

    adapter_instance_id     STRING      NOT NULL
        COMMENT 'Unique adapter instance identifier. Example: nvd_prod_01, tenable_prod_us_01.',

    source_system           STRING      NOT NULL
        COMMENT 'Source system enum. Example: NVD, TENABLE, SYSDIG, QUALYS.',

    adapter_type            STRING      NOT NULL
        COMMENT 'API/adapter implementation type. Example: REST_API, FILE, STREAM.',

    environment             STRING
        COMMENT 'Environment name. Example: DEV, UAT, PROD.',


    -- ---------------------------------------------------------------
    -- Connection / Endpoint
    -- ---------------------------------------------------------------
    base_url                STRING
        COMMENT 'Base API URL.',

    auth_method             STRING
        COMMENT 'Authentication type. Example: API_KEY, BASIC_AUTH, OAUTH, NONE.',

    auth_secret_ref         STRING
        COMMENT 'Secret reference path. Example: vault://nvd/api_key.',


    -- ---------------------------------------------------------------
    -- Ingestion Behaviour
    -- ---------------------------------------------------------------
    ingestion_mode          STRING      NOT NULL
        COMMENT 'Supported source ingestion strategy: FULL | INCREMENTAL | SNAPSHOT.',

    schedule_cron           STRING
        COMMENT 'Cron schedule for orchestration.',

    schedule_enabled        BOOLEAN
        COMMENT 'Whether scheduler is enabled for this adapter.',

    sla_minutes             INT
        COMMENT 'Expected completion SLA in minutes.',


    -- ---------------------------------------------------------------
    -- Runtime Configuration
    -- ---------------------------------------------------------------
    pagination_config_json  STRING
        COMMENT 'Pagination configuration JSON. Example: {"type":"offset","page_size":2000,"max_pages":null}',

    runtime_config_json     STRING
        COMMENT 'Generic runtime configuration JSON (retry, timeout, rate limits etc.). Example: {"timeout_sec":120,"max_retries":3,"rate_limit_rps":5}',

    path_template           STRING
        COMMENT 'Raw landing path template in HDFS. Example: vulnerability_intel/nvd/raw/ingestion_date=${ingestion_date}/batch_id=${batch_id}',


    -- ---------------------------------------------------------------
    -- Lifecycle
    -- ---------------------------------------------------------------
    is_active               BOOLEAN
        COMMENT 'Whether adapter is active (FALSE = decommissioned).',

    created_at              TIMESTAMP
        COMMENT 'Creation timestamp.',

    updated_at              TIMESTAMP
        COMMENT 'Last update timestamp.'

)
USING iceberg

PARTITIONED BY (
    source_system
)

TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'comment' = 'UEH adapter configuration metadata table.'
);



-- =====================================================================
-- 2. ADAPTER STATE TABLE
-- =====================================================================
-- "Where did I stop last time?"
-- NiFi reads: watermark_state_json before calling API
-- NiFi updates: watermark_state_json after successful ingestion
--
-- NOTE: NO PARTITION — table has <100 rows, state_status is mutable.
--       Partitioning on mutable field causes delete-file churn in Iceberg.
-- =====================================================================

CREATE TABLE IF NOT EXISTS t01_ueh_dev_ctl.t01_ueh_ctl_adapter_state (

    -- ---------------------------------------------------------------
    -- Identity
    -- ---------------------------------------------------------------
    org_id                  STRING      NOT NULL
        COMMENT 'Logical organisation/tenant identifier.',

    adapter_instance_id     STRING      NOT NULL
        COMMENT 'FK -> t01_ueh_ctl_adapter_config.adapter_instance_id.',


    -- ---------------------------------------------------------------
    -- Watermark State
    -- ---------------------------------------------------------------
    watermark_state_json    STRING
        COMMENT 'Adapter-specific watermark JSON. Supports timestamp, cursor, export ID etc. Example: {"lastModStartDate":"2024-01-01T00:00:00.000","watermark_type":"iso_datetime"}',

    last_batch_id           STRING
        COMMENT 'Last successfully executed batch ID.',


    -- ---------------------------------------------------------------
    -- Operational Tracking
    -- ---------------------------------------------------------------
    last_successful_run     TIMESTAMP
        COMMENT 'Last successful execution timestamp.',

    records_last_pulled     BIGINT
        COMMENT 'Number of records pulled in last successful execution.',

    consecutive_failures    INT
        COMMENT 'Count of consecutive failures. Resets to 0 on success.',

    state_status            STRING
        COMMENT 'NEW | HEALTHY | FAILING | DISABLED',


    -- ---------------------------------------------------------------
    -- Failure Tracking
    -- ---------------------------------------------------------------
    last_failure_reason     STRING
        COMMENT 'Last failure reason if execution failed.',

    updated_at              TIMESTAMP
        COMMENT 'Last state update timestamp.'

)
USING iceberg

-- NO PARTITION: Table has <100 rows. state_status is mutable — partitioning
-- on a mutable column causes Iceberg delete-file churn and small-file problems.

TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'comment' = 'UEH adapter runtime execution state table. Unpartitioned due to small size and mutable status field.'
);



-- =====================================================================
-- 3. BATCH REGISTRY TABLE
-- =====================================================================
-- "What happened in each run?"
-- NiFi writes: batch_status = 'RAW_COMPLETE' after HDFS write
-- Spark writes: batch_status = 'BRONZE_COMPLETE' after Iceberg load
-- DAG 2 polls: WHERE batch_status = 'RAW_COMPLETE' (coupling mechanism)
--
-- ADDED from review: bronze_path, ingestion_date, chunks_written,
--                    failure_category, parent_batch_id
-- =====================================================================

CREATE TABLE IF NOT EXISTS t01_ueh_dev_ctl.t01_ueh_ctl_batch_registry (

    -- ---------------------------------------------------------------
    -- Identity
    -- ---------------------------------------------------------------
    org_id                  STRING      NOT NULL
        COMMENT 'Logical organisation/tenant identifier.',

    batch_id                STRING      NOT NULL
        COMMENT 'Unique batch identifier. Pattern: batch_{yyyyMMddHHmmss}_{adapter_instance_id}',

    adapter_instance_id     STRING      NOT NULL
        COMMENT 'FK -> adapter configuration.',


    -- ---------------------------------------------------------------
    -- Execution Context
    -- ---------------------------------------------------------------
    trigger_type            STRING
        COMMENT 'SCHEDULED | MANUAL | REPLAY | EVENT_DRIVEN',

    load_type               STRING
        COMMENT 'FULL_LOAD | INCREMENTAL | SNAPSHOT | REPLAY',

    batch_status            STRING
        COMMENT 'INITIATED | RUNNING | RAW_COMPLETE | BRONZE_COMPLETE | FAILED | PARTIAL_SUCCESS | REPLAY',

    ingestion_date          DATE
        COMMENT 'Logical ingestion date. May differ from created_at for backfills/replays.',


    -- ---------------------------------------------------------------
    -- Paths
    -- ---------------------------------------------------------------
    bronze_path             STRING
        COMMENT 'Actual resolved HDFS path where raw chunks were written by NiFi.',

    checkpoint_path         STRING
        COMMENT 'Path to checkpoint.json generated by NiFi.',


    -- ---------------------------------------------------------------
    -- Watermark / Runtime Context
    -- ---------------------------------------------------------------
    watermark_state_json    STRING
        COMMENT 'Execution watermark context JSON. What range this batch covers.',


    -- ---------------------------------------------------------------
    -- Processing Metrics
    -- ---------------------------------------------------------------
    records_expected        BIGINT
        COMMENT 'Expected record count from source API (totalResults).',

    records_processed       BIGINT
        COMMENT 'Successfully processed records count.',

    chunks_written          INT
        COMMENT 'Number of chunk files written to HDFS.',


    -- ---------------------------------------------------------------
    -- Timing
    -- ---------------------------------------------------------------
    start_time              TIMESTAMP
        COMMENT 'Batch start timestamp.',

    end_time                TIMESTAMP
        COMMENT 'Batch completion timestamp.',


    -- ---------------------------------------------------------------
    -- Failure Tracking
    -- ---------------------------------------------------------------
    failure_reason          STRING
        COMMENT 'Failure reason text if batch failed.',

    failure_category        STRING
        COMMENT 'Standardized failure type: AUTH_FAILURE | API_TIMEOUT | RATE_LIMIT | BAD_PAYLOAD | SCHEMA_DRIFT | NETWORK_FAILURE | PARSE_ERROR | INTERNAL_ERROR',


    -- ---------------------------------------------------------------
    -- Lineage
    -- ---------------------------------------------------------------
    parent_batch_id         STRING
        COMMENT 'If REPLAY, reference to original batch_id for lineage.',


    -- ---------------------------------------------------------------
    -- Audit
    -- ---------------------------------------------------------------
    created_at              TIMESTAMP
        COMMENT 'Batch creation timestamp.'

)
USING iceberg

PARTITIONED BY (
    days(created_at)
)

TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728',
    'comment' = 'UEH orchestration and execution tracking table.'
);



-- =====================================================================
-- VERIFICATION
-- =====================================================================
SHOW TABLES IN t01_ueh_dev_ctl;
