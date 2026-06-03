#!/bin/bash
# =============================================================================
# UEH Platform: HDFS Folder Structure Initialization
# =============================================================================
# Purpose: Create the foundational HDFS directory structure for UEH
# Usage:   ./scripts/hdfs_init.sh <environment>
# Example: ./scripts/hdfs_init.sh dev
#
# This script creates:
#   1. Data lake Bronze raw storage folders
#   2. Dead letter folders (mirror Bronze structure)
#   3. Warehouse Iceberg storage paths
#   4. Metadata/schema_versions folders
#
# NOTE: Sub-folders (ingestion_date, batch_id) are auto-created at runtime
#       by NiFi's PutHDFS processor. Only the base structure is pre-created.
# =============================================================================

set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

ENV="${1:-}"

if [[ -z "$ENV" ]]; then
    echo "ERROR: Environment argument required."
    echo "Usage: $0 <dev|uat|prod>"
    exit 1
fi

if [[ "$ENV" != "dev" && "$ENV" != "uat" && "$ENV" != "prod" ]]; then
    echo "ERROR: Invalid environment '$ENV'. Must be: dev, uat, or prod"
    exit 1
fi

echo "=============================================="
echo "UEH HDFS Initialization: ${ENV} environment"
echo "=============================================="
echo ""

# Base paths
DATA_LAKE_BASE="/data-lake/${ENV}/bronze"
WAREHOUSE_BASE="/warehouse/${ENV}"

# ─────────────────────────────────────────────────────────────────────────────
# 1. Data Lake: Bronze Raw Storage
# ─────────────────────────────────────────────────────────────────────────────
echo "[1/4] Creating Bronze raw storage structure..."

# --- Vulnerability Intelligence ---
echo "  → vulnerability_intel/nvd"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/nvd/metadata/schema_versions"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/nvd/raw"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/nvd/dead_letter"

echo "  → vulnerability_intel/epss"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/epss/metadata/schema_versions"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/epss/raw"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/epss/dead_letter"

echo "  → vulnerability_intel/cisa_kev"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/cisa_kev/metadata/schema_versions"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/cisa_kev/raw"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/cisa_kev/dead_letter"

echo "  → vulnerability_intel/msrc"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/msrc/metadata/schema_versions"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/msrc/raw"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/vulnerability_intel/msrc/dead_letter"

# --- Scanners ---
echo "  → scanners/tenable"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/tenable/metadata/schema_versions"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/tenable/raw"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/tenable/dead_letter"

echo "  → scanners/sysdig"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/sysdig/metadata/schema_versions"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/sysdig/raw"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/sysdig/dead_letter"

echo "  → scanners/guardium"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/guardium/metadata/schema_versions"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/guardium/raw"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/guardium/dead_letter"

echo "  → scanners/fortify"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/fortify/metadata/schema_versions"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/fortify/raw"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/scanners/fortify/dead_letter"

# --- Asset Inventory ---
echo "  → asset_inventory/bmc_addm"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/asset_inventory/bmc_addm/metadata/schema_versions"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/asset_inventory/bmc_addm/raw"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/asset_inventory/bmc_addm/dead_letter"

echo "  → asset_inventory/cmdb"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/asset_inventory/cmdb/metadata/schema_versions"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/asset_inventory/cmdb/raw"
hdfs dfs -mkdir -p "${DATA_LAKE_BASE}/asset_inventory/cmdb/dead_letter"

echo "  ✓ Bronze raw storage created."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 2. Warehouse: Iceberg Table Storage
# ─────────────────────────────────────────────────────────────────────────────
echo "[2/4] Creating Warehouse (Iceberg) storage paths..."

hdfs dfs -mkdir -p "${WAREHOUSE_BASE}/control"
hdfs dfs -mkdir -p "${WAREHOUSE_BASE}/bronze"
hdfs dfs -mkdir -p "${WAREHOUSE_BASE}/silver"
hdfs dfs -mkdir -p "${WAREHOUSE_BASE}/gold"

echo "  ✓ Warehouse paths created."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 3. Set Permissions
# ─────────────────────────────────────────────────────────────────────────────
echo "[3/4] Setting HDFS permissions..."

# Data lake: read-write for ingestion service account
hdfs dfs -chmod -R 770 "${DATA_LAKE_BASE}"
hdfs dfs -chown -R ueh_svc:ueh_team "${DATA_LAKE_BASE}" 2>/dev/null || \
    echo "  ⚠ Warning: Could not set ownership (may need superuser). Skipping."

# Warehouse: read-write for processing service account
hdfs dfs -chmod -R 770 "${WAREHOUSE_BASE}"
hdfs dfs -chown -R ueh_svc:ueh_team "${WAREHOUSE_BASE}" 2>/dev/null || \
    echo "  ⚠ Warning: Could not set ownership (may need superuser). Skipping."

echo "  ✓ Permissions set."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 4. Verification
# ─────────────────────────────────────────────────────────────────────────────
echo "[4/4] Verifying structure..."
echo ""
echo "=== Data Lake Bronze ==="
hdfs dfs -ls -R "${DATA_LAKE_BASE}" | head -40
echo ""
echo "=== Warehouse ==="
hdfs dfs -ls "${WAREHOUSE_BASE}"
echo ""

echo "=============================================="
echo "✓ UEH HDFS initialization complete for: ${ENV}"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. Run DDL:  spark-sql -f ddl/01_databases.sql"
echo "  2. Run DDL:  spark-sql -f ddl/02_control_tables.sql"
echo "  3. Run DDL:  spark-sql -f ddl/03_bronze_nvd.sql"
echo "  4. Seed:     spark-sql -f seed/01_nvd_adapter_seed.sql"
echo "  5. Deploy NiFi flow"
echo "  6. Deploy Airflow DAGs"
