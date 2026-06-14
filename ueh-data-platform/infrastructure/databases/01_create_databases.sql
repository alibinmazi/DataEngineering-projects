-- =============================================================================
-- UEH Data Platform: Database Creation
-- =============================================================================
-- Creates all Iceberg databases for dev/uat/prod environments.
-- Pattern: t01_ueh_{env}_{layer}
--
-- Execution: spark-sql -f infrastructure/databases/01_create_databases.sql
-- =============================================================================

-- DEV
CREATE DATABASE IF NOT EXISTS t01_ueh_dev_ctl
    COMMENT 'UEH DEV - Control plane (adapter configs, state, batch tracking)'
    LOCATION '/warehouse/dev/control';

CREATE DATABASE IF NOT EXISTS t01_ueh_dev_brz
    COMMENT 'UEH DEV - Bronze layer (raw immutable ingestion)'
    LOCATION '/warehouse/dev/bronze';

CREATE DATABASE IF NOT EXISTS t01_ueh_dev_slv
    COMMENT 'UEH DEV - Silver layer (standardized, mapped, quality-checked)'
    LOCATION '/warehouse/dev/silver';

CREATE DATABASE IF NOT EXISTS t01_ueh_dev_gld
    COMMENT 'UEH DEV - Gold layer (business-ready analytics)'
    LOCATION '/warehouse/dev/gold';

-- UAT
CREATE DATABASE IF NOT EXISTS t01_ueh_uat_ctl
    COMMENT 'UEH UAT - Control plane'
    LOCATION '/warehouse/uat/control';

CREATE DATABASE IF NOT EXISTS t01_ueh_uat_brz
    COMMENT 'UEH UAT - Bronze layer'
    LOCATION '/warehouse/uat/bronze';

CREATE DATABASE IF NOT EXISTS t01_ueh_uat_slv
    COMMENT 'UEH UAT - Silver layer'
    LOCATION '/warehouse/uat/silver';

CREATE DATABASE IF NOT EXISTS t01_ueh_uat_gld
    COMMENT 'UEH UAT - Gold layer'
    LOCATION '/warehouse/uat/gold';

-- PROD
CREATE DATABASE IF NOT EXISTS t01_ueh_prod_ctl
    COMMENT 'UEH PROD - Control plane'
    LOCATION '/warehouse/prod/control';

CREATE DATABASE IF NOT EXISTS t01_ueh_prod_brz
    COMMENT 'UEH PROD - Bronze layer'
    LOCATION '/warehouse/prod/bronze';

CREATE DATABASE IF NOT EXISTS t01_ueh_prod_slv
    COMMENT 'UEH PROD - Silver layer'
    LOCATION '/warehouse/prod/silver';

CREATE DATABASE IF NOT EXISTS t01_ueh_prod_gld
    COMMENT 'UEH PROD - Gold layer'
    LOCATION '/warehouse/prod/gold';

-- Verify
SHOW DATABASES LIKE 't01_ueh_*';
