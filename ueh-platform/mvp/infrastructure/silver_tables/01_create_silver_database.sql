-- =============================================================================
-- UEH Silver Layer: Database Creation
-- =============================================================================
-- Pattern: t01_ueh_{env}_slv
-- Run AFTER: infrastructure/databases/01_create_databases.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS t01_ueh_dev_slv
    COMMENT 'UEH DEV - Silver layer (normalized, mapped, validated, enriched)'
    LOCATION '/warehouse/dev/silver';
