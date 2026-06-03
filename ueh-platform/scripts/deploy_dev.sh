#!/bin/bash
# =============================================================================
# UEH Platform: Dev Environment Full Deployment Script
# =============================================================================
# Purpose: One-command deployment of UEH to dev environment
# Usage:   ./scripts/deploy_dev.sh
#
# This script:
#   1. Creates HDFS folder structure
#   2. Deploys Iceberg DDL (databases + tables)
#   3. Seeds adapter configuration
#   4. Deploys Spark jobs to HDFS
#   5. Deploys Airflow DAGs
#   6. Runs validation
#
# Prerequisites:
#   - HDFS access (kinit if using Kerberos)
#   - spark-sql available on PATH
#   - Airflow CLI available (or AIRFLOW_HOME set)
#   - HDFS write permissions
# =============================================================================

set -euo pipefail

ENV="dev"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  UEH Platform: Dev Environment Deployment               ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Environment: ${ENV}                                        ║"
echo "║  Project:     ${PROJECT_ROOT}   ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: HDFS Structure
# ─────────────────────────────────────────────────────────────────────────────
echo "┌──────────────────────────────────────────────────────────┐"
echo "│ Step 1/6: Creating HDFS folder structure                  │"
echo "└──────────────────────────────────────────────────────────┘"

"${PROJECT_ROOT}/scripts/hdfs_init.sh" "${ENV}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Create Databases
# ─────────────────────────────────────────────────────────────────────────────
echo "┌──────────────────────────────────────────────────────────┐"
echo "│ Step 2/6: Creating Iceberg databases                      │"
echo "└──────────────────────────────────────────────────────────┘"

spark-sql -f "${PROJECT_ROOT}/ddl/01_databases.sql" 2>&1 | tail -5
echo "  ✓ Databases created."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Create Tables
# ─────────────────────────────────────────────────────────────────────────────
echo "┌──────────────────────────────────────────────────────────┐"
echo "│ Step 3/6: Creating control tables and Bronze tables        │"
echo "└──────────────────────────────────────────────────────────┘"

spark-sql -f "${PROJECT_ROOT}/ddl/02_control_tables.sql" 2>&1 | tail -5
echo "  ✓ Control tables created."

spark-sql -f "${PROJECT_ROOT}/ddl/03_bronze_nvd.sql" 2>&1 | tail -5
echo "  ✓ Bronze NVD table created."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Seed Configuration
# ─────────────────────────────────────────────────────────────────────────────
echo "┌──────────────────────────────────────────────────────────┐"
echo "│ Step 4/6: Seeding adapter configuration                   │"
echo "└──────────────────────────────────────────────────────────┘"

spark-sql -f "${PROJECT_ROOT}/seed/01_nvd_adapter_seed.sql" 2>&1 | tail -5
echo "  ✓ NVD adapter configured."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Deploy Spark Jobs
# ─────────────────────────────────────────────────────────────────────────────
echo "┌──────────────────────────────────────────────────────────┐"
echo "│ Step 5/6: Deploying Spark jobs to HDFS                    │"
echo "└──────────────────────────────────────────────────────────┘"

SPARK_HDFS_PATH="/apps/ueh/spark/bronze"
hdfs dfs -mkdir -p "${SPARK_HDFS_PATH}"
hdfs dfs -put -f "${PROJECT_ROOT}/spark/bronze/base_bronze_loader.py" "${SPARK_HDFS_PATH}/"
hdfs dfs -put -f "${PROJECT_ROOT}/spark/bronze/bronze_nvd_loader.py" "${SPARK_HDFS_PATH}/"

echo "  ✓ Spark jobs deployed to ${SPARK_HDFS_PATH}"
hdfs dfs -ls "${SPARK_HDFS_PATH}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Deploy Airflow DAGs
# ─────────────────────────────────────────────────────────────────────────────
echo "┌──────────────────────────────────────────────────────────┐"
echo "│ Step 6/6: Deploying Airflow DAGs                          │"
echo "└──────────────────────────────────────────────────────────┘"

AIRFLOW_DAGS_DIR="${AIRFLOW_HOME:-/opt/airflow}/dags"

if [ -d "$AIRFLOW_DAGS_DIR" ]; then
    cp "${PROJECT_ROOT}/airflow/dags/ueh_ingest_nvd.py" "${AIRFLOW_DAGS_DIR}/"
    cp "${PROJECT_ROOT}/airflow/dags/ueh_bronze_load_nvd.py" "${AIRFLOW_DAGS_DIR}/"
    echo "  ✓ DAGs deployed to ${AIRFLOW_DAGS_DIR}"
else
    echo "  ⚠ AIRFLOW_HOME/dags not found at ${AIRFLOW_DAGS_DIR}"
    echo "    Manually copy DAGs from: ${PROJECT_ROOT}/airflow/dags/"
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  ✓ UEH Dev Deployment COMPLETE                          ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║                                                          ║"
echo "║  Next Steps:                                             ║"
echo "║  1. Deploy NiFi flow (see nifi/nvd_ingestion_flow.md)   ║"
echo "║  2. Store NVD API key in vault                           ║"
echo "║  3. Set Airflow variables:                               ║"
echo "║     - ueh_environment = dev                              ║"
echo "║     - ueh_nifi_base_url = <nifi-url>                    ║"
echo "║     - ueh_nifi_pg_nvd = <process-group-id>              ║"
echo "║  4. Enable DAGs in Airflow UI                            ║"
echo "║  5. Trigger first run: airflow dags trigger ueh_ingest_nvd║"
echo "║  6. Validate: spark-sql -f validation/bronze_nvd_checks.sql║"
echo "║                                                          ║"
echo "╚══════════════════════════════════════════════════════════╝"
