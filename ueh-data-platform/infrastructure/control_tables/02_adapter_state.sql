-- =============================================================================
-- UEH Control Table: Adapter State
-- =============================================================================
-- Written by: NiFi (after ingestion), Spark (after processing)
-- Read by: NiFi (watermark), Airflow (health check)
-- Purpose: "Where did I stop? Is the adapter healthy?"
-- NO PARTITION: <100 rows, mutable state_status field
-- =============================================================================

USE t01_ueh_dev_ctl;

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_adapter_state (

    org_id                  STRING      NOT NULL
        COMMENT 'Organisation/tenant identifier.',
    adapter_instance_id     STRING      NOT NULL
        COMMENT 'FK → adapter_config.',

    watermark_state_json    STRING
        COMMENT 'Structured watermark. Example: {"lastModStartDate":"2024-01-01T00:00:00.000","watermark_type":"iso_datetime"}',
    last_batch_id           STRING
        COMMENT 'Last successfully completed batch ID.',

    last_successful_run     TIMESTAMP
        COMMENT 'Last successful run completion time.',
    records_last_pulled     BIGINT
        COMMENT 'Records in last successful run.',
    consecutive_failures    INT
        COMMENT 'Failure streak count (resets on success).',
    state_status            STRING
        COMMENT 'NEW | HEALTHY | FAILING | DISABLED',

    last_failure_reason     STRING
        COMMENT 'Most recent failure message.',
    updated_at              TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);
