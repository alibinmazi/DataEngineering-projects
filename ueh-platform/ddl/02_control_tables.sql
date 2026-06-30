-- =============================================================================
-- UEH Platform: Control Framework Tables
-- =============================================================================
-- These tables form the metadata-driven orchestration backbone of UEH.
-- All runtime decisions (what to ingest, where to resume, what failed) are
-- driven by querying these tables — NOT by checking file existence.
--
-- Execution: spark-sql -f ddl/02_control_tables.sql
-- Note: Replace ${env} with target environment (dev/uat/prod)
-- =============================================================================

USE ueh_dev_control;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. ADAPTER CONFIGURATION
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose: Registry of all source adapters and their connection/behavior config
-- Read by: NiFi (ingestion), Airflow (scheduling), Spark (loading)
-- One row per adapter INSTANCE (not per adapter type)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_adapter_config (
    -- Identity
    adapter_instance_id     STRING      NOT NULL    COMMENT 'Globally unique instance ID (PK). Pattern: {adapter_name}_{env}_{region}_{seq}',
    adapter_name            STRING      NOT NULL    COMMENT 'Adapter name: nvd, epss, tenable, sysdig, etc.',
    adapter_type            STRING      NOT NULL    COMMENT 'Category: scanner, vulnerability_intel, asset_inventory',
    org_id                  STRING                  COMMENT 'Organization ID. NULL for global feeds (NVD, EPSS)',
    
    -- Connection
    region                  STRING                  COMMENT 'Deployment region: us-east, eu-west, ap-southeast',
    base_url                STRING      NOT NULL    COMMENT 'API base URL',
    auth_method             STRING      NOT NULL    COMMENT 'Authentication: none, api_key, oauth2, certificate, basic',
    auth_secret_ref         STRING                  COMMENT 'Vault/secret manager reference for credentials',
    
    -- Ingestion Behavior
    ingestion_mode          STRING      NOT NULL    COMMENT 'What source supports: INCREMENTAL, SNAPSHOT, FULL, HYBRID',
    default_load_type       STRING      NOT NULL    COMMENT 'Default load_type for scheduled runs: INCREMENTAL, FULL_LOAD, SNAPSHOT',
    schedule_cron           STRING                  COMMENT 'Cron expression for scheduled execution',
    chunk_size              INT                     COMMENT 'Max records per chunk file on HDFS',
    page_size               INT                     COMMENT 'API pagination page size',
    rate_limit_rps          INT                     COMMENT 'Max requests per second to source API',
    request_timeout_sec     INT         DEFAULT 300 COMMENT 'HTTP request timeout in seconds',
    max_retries             INT         DEFAULT 3   COMMENT 'Max retry attempts per API call',
    
    -- Path Configuration
    path_template           STRING      NOT NULL    COMMENT 'HDFS path template with ${placeholders}',
    
    -- Operational
    sla_minutes             INT                     COMMENT 'Max allowed ingestion duration before SLA breach alert',
    is_active               BOOLEAN     NOT NULL    COMMENT 'Active flag. FALSE = skipped by orchestration',
    priority                INT         DEFAULT 5   COMMENT 'Execution priority (1=highest, 10=lowest)',
    onboarded_date          DATE                    COMMENT 'Date this adapter instance was onboarded',
    decommissioned_date     DATE                    COMMENT 'Date decommissioned (NULL if active)',
    owner_team              STRING                  COMMENT 'Team responsible for this adapter',
    notes                   STRING                  COMMENT 'Free-text operational notes',
    
    -- Audit
    created_at              TIMESTAMP   NOT NULL    COMMENT 'Record creation timestamp',
    updated_at              TIMESTAMP   NOT NULL    COMMENT 'Last modification timestamp',
    created_by              STRING                  COMMENT 'Who created this record',
    updated_by              STRING                  COMMENT 'Who last modified this record'
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. ADAPTER STATE
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose: Runtime state tracking per adapter instance
-- Contains: watermarks, cursors, health status
-- Updated: After every successful/failed ingestion run
-- Used by: NiFi (to know where to resume), Airflow (health monitoring)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_adapter_state (
    -- Identity
    adapter_instance_id     STRING      NOT NULL    COMMENT 'FK to adapter_config (PK here)',
    
    -- Watermark / Cursor
    watermark_value         STRING                  COMMENT 'Last successful sync point value',
    watermark_type          STRING                  COMMENT 'Type: unix_timestamp, iso_datetime, page_token, export_uuid, offset',
    watermark_field         STRING                  COMMENT 'Source field used as watermark (e.g., lastModifiedDate)',
    
    -- Last Run Info
    last_successful_run     TIMESTAMP               COMMENT 'Timestamp of last successful completion',
    last_attempted_run      TIMESTAMP               COMMENT 'Timestamp of last attempt (success or failure)',
    records_last_pulled     BIGINT      DEFAULT 0   COMMENT 'Records retrieved in most recent successful run',
    
    -- Health
    consecutive_failures    INT         DEFAULT 0   COMMENT 'Count of consecutive failures (resets on success)',
    state_status            STRING      NOT NULL    COMMENT 'HEALTHY, DEGRADED, FAILING, NEW, PAUSED',
    last_failure_reason     STRING                  COMMENT 'Error message from most recent failure',
    
    -- Circuit Breaker
    circuit_breaker_open    BOOLEAN     DEFAULT FALSE COMMENT 'TRUE = stop attempting until manual reset',
    circuit_breaker_opened_at TIMESTAMP             COMMENT 'When circuit breaker was opened',
    
    -- Audit
    last_updated            TIMESTAMP   NOT NULL    COMMENT 'State last updated timestamp'
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. BATCH REGISTRY
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose: Complete execution history of every batch across all layers
-- Lifecycle: RAW_COMPLETE → BRONZE_COMPLETE → SILVER_COMPLETE → GOLD_COMPLETE
-- Used by: DAG 2 sensor (polls for RAW_COMPLETE), lineage, replay, auditing
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_batch_registry (
    -- Identity
    batch_id                STRING      NOT NULL    COMMENT 'Unique batch ID (PK). Pattern: batch_{timestamp}_{adapter_instance_id}',
    adapter_instance_id     STRING      NOT NULL    COMMENT 'FK to adapter_config',
    adapter_name            STRING      NOT NULL    COMMENT 'Denormalized for query convenience',
    org_id                  STRING                  COMMENT 'Organization ID (NULL for global feeds)',
    
    -- Batch Context
    ingestion_date          DATE        NOT NULL    COMMENT 'Logical ingestion date (partition key)',
    load_type               STRING      NOT NULL    COMMENT 'What THIS run executed: FULL_LOAD, INCREMENTAL, SNAPSHOT, REPLAY',
    
    -- Status Lifecycle
    status                  STRING      NOT NULL    COMMENT 'RAW_COMPLETE, BRONZE_COMPLETE, SILVER_COMPLETE, GOLD_COMPLETE, FAILED, DEAD_LETTERED',
    
    -- Metrics
    records_ingested        BIGINT                  COMMENT 'Total records in this batch',
    chunks_written          INT                     COMMENT 'Number of chunk files written to HDFS',
    bytes_written           BIGINT                  COMMENT 'Total bytes written',
    
    -- Paths
    bronze_path             STRING                  COMMENT 'Full HDFS path to raw batch data',
    dead_letter_path        STRING                  COMMENT 'HDFS path if batch was dead-lettered',
    
    -- Watermark
    watermark_start         STRING                  COMMENT 'Watermark value at start of this batch',
    watermark_end           STRING                  COMMENT 'New watermark value after this batch',
    
    -- Timing
    started_at              TIMESTAMP               COMMENT 'Batch execution start',
    raw_completed_at        TIMESTAMP               COMMENT 'Raw ingestion completed (NiFi done)',
    bronze_completed_at     TIMESTAMP               COMMENT 'Bronze Iceberg load completed',
    silver_completed_at     TIMESTAMP               COMMENT 'Silver transformation completed',
    gold_completed_at       TIMESTAMP               COMMENT 'Gold aggregation completed',
    
    -- Error Handling
    failure_reason          STRING                  COMMENT 'Error message if status=FAILED',
    failure_stage           STRING                  COMMENT 'Stage where failure occurred: RAW, BRONZE, SILVER, GOLD',
    retry_count             INT         DEFAULT 0   COMMENT 'Number of retry attempts',
    
    -- Lineage
    parent_batch_id         STRING                  COMMENT 'If this is a replay, reference to original batch',
    triggered_by            STRING                  COMMENT 'What triggered this batch: SCHEDULE, MANUAL, REPLAY, BACKFILL',
    
    -- Audit
    created_at              TIMESTAMP   NOT NULL    COMMENT 'Record creation timestamp',
    updated_at              TIMESTAMP   NOT NULL    COMMENT 'Last status update timestamp'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. INGESTION LOG
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose: Detailed append-only log of ingestion events
-- Granularity: One row per significant event (API call, chunk write, error)
-- Used for: Debugging, performance analysis, audit trail
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_ingestion_log (
    -- Identity
    log_id                  STRING      NOT NULL    COMMENT 'Unique log entry ID',
    batch_id                STRING      NOT NULL    COMMENT 'FK to batch_registry',
    adapter_instance_id     STRING      NOT NULL    COMMENT 'FK to adapter_config',
    
    -- Event
    event_timestamp         TIMESTAMP   NOT NULL    COMMENT 'When this event occurred',
    event_type              STRING      NOT NULL    COMMENT 'API_CALL, CHUNK_WRITTEN, PAGE_FETCHED, ERROR, RETRY, RATE_LIMITED',
    event_detail            STRING                  COMMENT 'Detailed event description',
    
    -- API Call Details (populated for API_CALL events)
    http_method             STRING                  COMMENT 'GET, POST',
    http_url                STRING                  COMMENT 'Full request URL',
    http_status_code        INT                     COMMENT 'Response status code',
    response_time_ms        BIGINT                  COMMENT 'Response time in milliseconds',
    records_in_response     INT                     COMMENT 'Records returned in this response',
    
    -- Chunk Details (populated for CHUNK_WRITTEN events)
    chunk_filename          STRING                  COMMENT 'Chunk file written',
    chunk_size_bytes        BIGINT                  COMMENT 'Chunk file size',
    records_in_chunk        INT                     COMMENT 'Records in this chunk',
    
    -- Error Details (populated for ERROR events)
    error_code              STRING                  COMMENT 'Error classification code',
    error_message           STRING                  COMMENT 'Error message',
    is_retryable            BOOLEAN                 COMMENT 'Whether this error is retryable',
    
    -- Partition
    ingestion_date          DATE        NOT NULL    COMMENT 'Partition key'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. FAILED INGESTIONS (Dead Letter Registry)
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose: Track all failed/dead-lettered ingestion attempts
-- Used for: Replay queue, operational monitoring, forensics
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_failed_ingestions (
    -- Identity
    failure_id              STRING      NOT NULL    COMMENT 'Unique failure ID (PK)',
    batch_id                STRING      NOT NULL    COMMENT 'FK to batch_registry',
    adapter_instance_id     STRING      NOT NULL    COMMENT 'FK to adapter_config',
    
    -- Failure Context
    failure_timestamp       TIMESTAMP   NOT NULL    COMMENT 'When failure occurred',
    failure_stage           STRING      NOT NULL    COMMENT 'RAW_INGESTION, BRONZE_LOAD, SILVER_TRANSFORM',
    failure_reason          STRING      NOT NULL    COMMENT 'Error message / exception',
    failure_category        STRING                  COMMENT 'AUTH, TIMEOUT, RATE_LIMIT, SCHEMA, NETWORK, UNKNOWN',
    
    -- Dead Letter
    dead_letter_path        STRING                  COMMENT 'HDFS path where failed payload is stored',
    payload_preserved       BOOLEAN     DEFAULT TRUE COMMENT 'Whether raw payload was successfully preserved',
    records_affected        BIGINT                  COMMENT 'Number of records affected by this failure',
    
    -- Resolution
    resolution_status       STRING      NOT NULL    COMMENT 'PENDING, REPLAYED, RESOLVED, IGNORED',
    resolved_at             TIMESTAMP               COMMENT 'When this failure was resolved',
    resolved_by             STRING                  COMMENT 'Who resolved it',
    resolution_notes        STRING                  COMMENT 'Notes on resolution',
    replay_batch_id         STRING                  COMMENT 'New batch_id if replayed',
    
    -- Audit
    created_at              TIMESTAMP   NOT NULL    COMMENT 'Record creation timestamp',
    updated_at              TIMESTAMP   NOT NULL    COMMENT 'Last update timestamp'
)
USING iceberg
PARTITIONED BY (months(failure_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. REPLAY QUEUE
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose: Queue of batches to be replayed/reprocessed
-- Used by: Replay DAG that picks up pending replays
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_replay_queue (
    -- Identity
    replay_id               STRING      NOT NULL    COMMENT 'Unique replay request ID (PK)',
    
    -- What to Replay
    original_batch_id       STRING      NOT NULL    COMMENT 'Original batch to replay',
    adapter_instance_id     STRING      NOT NULL    COMMENT 'FK to adapter_config',
    replay_from_stage       STRING      NOT NULL    COMMENT 'Where to start replay: RAW (re-ingest), BRONZE (re-load), SILVER (re-transform)',
    
    -- Replay Config
    replay_reason           STRING      NOT NULL    COMMENT 'Why replay: SCHEMA_FIX, LOGIC_CHANGE, FAILURE_RECOVERY, BACKFILL',
    priority                INT         DEFAULT 5   COMMENT 'Replay priority (1=highest)',
    
    -- Status
    replay_status           STRING      NOT NULL    COMMENT 'PENDING, IN_PROGRESS, COMPLETED, FAILED',
    new_batch_id            STRING                  COMMENT 'Batch ID of the replay execution',
    
    -- Timing
    requested_at            TIMESTAMP   NOT NULL    COMMENT 'When replay was requested',
    started_at              TIMESTAMP               COMMENT 'When replay execution started',
    completed_at            TIMESTAMP               COMMENT 'When replay completed',
    
    -- Audit
    requested_by            STRING      NOT NULL    COMMENT 'Who requested the replay',
    notes                   STRING                  COMMENT 'Additional context'
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
);


-- =============================================================================
-- Verification
-- =============================================================================
SHOW TABLES IN ueh_dev_control;
