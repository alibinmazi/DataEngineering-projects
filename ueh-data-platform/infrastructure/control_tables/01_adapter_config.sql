-- =============================================================================
-- UEH Control Table: Adapter Configuration
-- =============================================================================
-- Written by: UEH Dashboard UI (via API)
-- Read by: NiFi, Airflow, Spark
-- Purpose: "What to ingest, how, and from where"
-- =============================================================================

USE t01_ueh_dev_ctl;

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_adapter_config (

    org_id                  STRING      NOT NULL
        COMMENT 'Organisation/tenant identifier.',
    adapter_instance_id     STRING      NOT NULL
        COMMENT 'Unique instance ID. Pattern: {source}_{env}_{region}_{seq}',
    source_system           STRING      NOT NULL
        COMMENT 'Source system enum: NVD, TENABLE, SYSDIG, QUALYS, EPSS, CISA_KEV.',
    adapter_type            STRING      NOT NULL
        COMMENT 'Implementation type: REST_API, FILE, STREAM.',
    environment             STRING
        COMMENT 'Environment: DEV, UAT, PROD.',

    base_url                STRING
        COMMENT 'API base URL.',
    auth_method             STRING
        COMMENT 'Authentication: API_KEY, BASIC_AUTH, OAUTH, NONE.',
    auth_secret_ref         STRING
        COMMENT 'Vault secret reference. Example: vault://secrets/ueh/prod/tenable_us',

    ingestion_mode          STRING      NOT NULL
        COMMENT 'Source capability: FULL | INCREMENTAL | SNAPSHOT.',
    schedule_cron           STRING
        COMMENT 'Cron schedule expression.',
    schedule_enabled        BOOLEAN
        COMMENT 'Whether scheduled runs are active.',
    sla_minutes             INT
        COMMENT 'Max allowed ingestion duration.',

    pagination_config_json  STRING
        COMMENT 'Pagination config: {"type":"offset","page_size":2000}',
    runtime_config_json     STRING
        COMMENT 'Runtime config: {"timeout_sec":120,"max_retries":3,"rate_limit_rps":5}',
    path_template           STRING
        COMMENT 'HDFS path template with ${placeholders}.',

    is_active               BOOLEAN
        COMMENT 'Active adapter (FALSE = decommissioned).',
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
)
USING iceberg
PARTITIONED BY (source_system)
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);
