-- =============================================================================
-- UEH Platform: Database/Namespace Creation
-- =============================================================================
-- Environment separation at database level (NOT table level)
-- Pattern: ueh_{env}_{layer}
-- 
-- Execution: spark-sql -f ddl/01_databases.sql --conf ueh.environment=dev
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- DEV Environment
-- ─────────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS ueh_dev_control
    COMMENT 'UEH DEV - Control plane (orchestration metadata, adapter configs, batch tracking)'
    LOCATION '/warehouse/dev/control';

CREATE DATABASE IF NOT EXISTS ueh_dev_bronze
    COMMENT 'UEH DEV - Bronze layer (raw immutable ingestion records)'
    LOCATION '/warehouse/dev/bronze';

CREATE DATABASE IF NOT EXISTS ueh_dev_silver
    COMMENT 'UEH DEV - Silver layer (standardized, normalized, quality-checked)'
    LOCATION '/warehouse/dev/silver';

CREATE DATABASE IF NOT EXISTS ueh_dev_gold
    COMMENT 'UEH DEV - Gold layer (business-ready analytics, curated datasets)'
    LOCATION '/warehouse/dev/gold';


-- ─────────────────────────────────────────────────────────────────────────────
-- UAT Environment
-- ─────────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS ueh_uat_control
    COMMENT 'UEH UAT - Control plane (orchestration metadata, adapter configs, batch tracking)'
    LOCATION '/warehouse/uat/control';

CREATE DATABASE IF NOT EXISTS ueh_uat_bronze
    COMMENT 'UEH UAT - Bronze layer (raw immutable ingestion records)'
    LOCATION '/warehouse/uat/bronze';

CREATE DATABASE IF NOT EXISTS ueh_uat_silver
    COMMENT 'UEH UAT - Silver layer (standardized, normalized, quality-checked)'
    LOCATION '/warehouse/uat/silver';

CREATE DATABASE IF NOT EXISTS ueh_uat_gold
    COMMENT 'UEH UAT - Gold layer (business-ready analytics, curated datasets)'
    LOCATION '/warehouse/uat/gold';


-- ─────────────────────────────────────────────────────────────────────────────
-- PROD Environment
-- ─────────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS ueh_prod_control
    COMMENT 'UEH PROD - Control plane (orchestration metadata, adapter configs, batch tracking)'
    LOCATION '/warehouse/prod/control';

CREATE DATABASE IF NOT EXISTS ueh_prod_bronze
    COMMENT 'UEH PROD - Bronze layer (raw immutable ingestion records)'
    LOCATION '/warehouse/prod/bronze';

CREATE DATABASE IF NOT EXISTS ueh_prod_silver
    COMMENT 'UEH PROD - Silver layer (standardized, normalized, quality-checked)'
    LOCATION '/warehouse/prod/silver';

CREATE DATABASE IF NOT EXISTS ueh_prod_gold
    COMMENT 'UEH PROD - Gold layer (business-ready analytics, curated datasets)'
    LOCATION '/warehouse/prod/gold';


-- =============================================================================
-- Verification
-- =============================================================================
SHOW DATABASES LIKE 'ueh_*';
