-- =============================================================================
-- UEH Control Table: Field Mapping
-- =============================================================================
-- Written by: UEH Dashboard UI (analyst maps source fields → canonical schema)
-- Read by: Spark Silver transformer (applies mappings dynamically)
-- Purpose: "How do I parse payload_json into structured Silver columns?"
-- =============================================================================

USE t01_ueh_dev_ctl;

CREATE TABLE IF NOT EXISTS t01_ueh_ctl_field_mapping (

    mapping_id              STRING      NOT NULL
        COMMENT 'Unique mapping rule ID.',
    org_id                  STRING      NOT NULL
        COMMENT 'Organisation.',
    source_system           STRING      NOT NULL
        COMMENT 'Source: NVD, TENABLE, SYSDIG.',
    mapping_version         INT         NOT NULL
        COMMENT 'Version number (increment on change).',

    source_json_path        STRING      NOT NULL
        COMMENT 'JSONPath to extract from payload_json. Example: $.cve.id',
    target_field            STRING      NOT NULL
        COMMENT 'Canonical Silver column name. Example: cve_id, severity',
    target_schema           STRING      NOT NULL
        COMMENT 'Target canonical schema: vulnerability, asset, threat_intel',

    transformation_type     STRING
        COMMENT 'DIRECT | CAST | LOOKUP | EXPRESSION | CUSTOM',
    transformation_config   STRING
        COMMENT 'Config JSON. Example: {"cast_to":"TIMESTAMP","format":"yyyy-MM-dd"}',

    is_required             BOOLEAN     DEFAULT FALSE
        COMMENT 'Whether this field must be non-null in Silver.',
    is_active               BOOLEAN     DEFAULT TRUE,

    created_by              STRING
        COMMENT 'Analyst who created this mapping.',
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
