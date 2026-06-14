#!/bin/bash
# =============================================================================
# Deploy Airflow DAGs
# Usage: ./scripts/deploy/deploy_airflow_dags.sh <env>
# =============================================================================

set -euo pipefail
ENV="${1:-dev}"

AIRFLOW_DAGS_DIR="${AIRFLOW_HOME:-/opt/airflow}/dags/ueh"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "Deploying Airflow DAGs: env=${ENV}, target=${AIRFLOW_DAGS_DIR}"

mkdir -p "${AIRFLOW_DAGS_DIR}"

# Copy all DAGs
cp -r "${PROJECT_ROOT}/orchestration/dags/"* "${AIRFLOW_DAGS_DIR}/"

# Copy plugins
PLUGINS_DIR="${AIRFLOW_HOME:-/opt/airflow}/plugins/ueh"
mkdir -p "${PLUGINS_DIR}"
cp "${PROJECT_ROOT}/orchestration/plugins/"*.py "${PLUGINS_DIR}/" 2>/dev/null || true

echo "✓ DAGs deployed to ${AIRFLOW_DAGS_DIR}"
ls -la "${AIRFLOW_DAGS_DIR}/"
