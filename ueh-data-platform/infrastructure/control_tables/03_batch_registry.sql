-- =============================================================================
-- UEH Control Table: Batch Registry
-- =============================================================================
-- Written by: NiFi (RAW_COMPLETE), Spark (BRONZE_COMPLETE, SILVER_COMPLETE)
-- Read by: Airflow DAGs (poll for status transitions)
-- Purpose: "What happened in each pipeline execution?"
-- Coupling mechanism: DAG 1 → RAW_COMPLETE → DAG 2 → BRONZE_COMPLETE → DAG 3
-- =============================================================================

USE t01_ueh_dev_ctl;

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_batch_registry (

    org_id                  STRING      NOT NULL
        COMMENT 'Organisation/tenant identifier.',
    batch_id                STRING      NOT NULL
        COMMENT 'Unique batch ID. Pattern: batch_{yyyyMMddHHmmss}_{adapter_instance_id}',
    adapter_instance_id     STRING      NOT NULL
        COMMENT 'FK → adapter_config.',

    trigger_type            STRING
        COMMENT 'SCHEDULED | MANUAL | REPLAY | EVENT_DRIVEN',
    load_type               STRING
        COMMENT 'FULL_LOAD | INCREMENTAL | SNAPSHOT | REPLAY',
    batch_status            STRING
        COMMENT 'INITIATED | RUNNING | RAW_COMPLETE | BRONZE_COMPLETE | SILVER_COMPLETE | GOLD_COMPLETE | FAILED',
    ingestion_date          DATE
        COMMENT 'Logical ingestion date (may differ from created_at for replays).',

    bronze_path             STRING
        COMMENT 'Resolved HDFS path where NiFi wrote raw chunks.',
    checkpoint_path         STRING
        COMMENT 'Path to checkpoint.json (resumability).',

    watermark_state_json    STRING
        COMMENT 'What range this batch covers (start/end watermarks).',

    records_expected        BIGINT
        COMMENT 'Expected records from source API (totalResults).',
    records_processed       BIGINT
        COMMENT 'Actually processed records.',
    chunks_written          INT
        COMMENT 'Number of chunk files on HDFS.',

    start_time              TIMESTAMP,
    end_time                TIMESTAMP,

    failure_reason          STRING
        COMMENT 'Error message if FAILED.',
    failure_category        STRING
        COMMENT 'AUTH_FAILURE | API_TIMEOUT | RATE_LIMIT | BAD_PAYLOAD | SCHEMA_DRIFT | NETWORK_FAILURE | PARSE_ERROR | INTERNAL_ERROR',

    parent_batch_id         STRING
        COMMENT 'Original batch_id if this is a REPLAY.',

    created_at              TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728'
);
