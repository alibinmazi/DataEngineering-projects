-- =============================================================================
-- UEH Gold Layer: Database Creation
-- =============================================================================
-- Pattern: t01_ueh_{env}_gld
-- Gold layer contains business-ready, enriched, aggregated datasets
-- consumed by dashboards, reports, chatbot, and analysts.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS t01_ueh_dev_gld
    COMMENT 'UEH DEV - Gold layer (enriched exposure analytics, business-ready)'
    LOCATION '/warehouse/dev/gold';
