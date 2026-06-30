-- =============================================================================
-- UEH Control Table: Schema Registry
-- =============================================================================
-- Written by: Platform team (defines canonical output schemas)
-- Read by: UI (shows available target fields), Silver transformer (validates)
-- Purpose: "What does a canonical vulnerability/asset record look like?"
-- =============================================================================

USE t01_ueh_dev_ctl;

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_schema_registry (

    schema_id               STRING      NOT NULL,
    schema_name             STRING      NOT NULL
        COMMENT 'Canonical schema: vulnerability, asset, threat_intel, exposure',
    schema_version          INT         NOT NULL
        COMMENT 'Version (increment on breaking change).',

    schema_definition_json  STRING      NOT NULL
        COMMENT 'Schema as JSON array: [{"field":"cve_id","type":"STRING","required":true,"description":"..."},...]',

    is_current              BOOLEAN     DEFAULT TRUE
        COMMENT 'Active version flag.',
    effective_from          DATE,
    deprecated_at           DATE,

    created_by              STRING,
    created_at              TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);
