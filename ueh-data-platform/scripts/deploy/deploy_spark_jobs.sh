#!/bin/bash
# =============================================================================
# Deploy Spark jobs to HDFS
# Usage: ./scripts/deploy/deploy_spark_jobs.sh <env>
# =============================================================================

set -euo pipefail
ENV="${1:-dev}"

SPARK_HDFS_PATH="/apps/ueh/spark"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "Deploying Spark jobs to HDFS: env=${ENV}, path=${SPARK_HDFS_PATH}"

hdfs dfs -mkdir -p "${SPARK_HDFS_PATH}"

# Deploy generic Bronze loader
hdfs dfs -put -f "${PROJECT_ROOT}/processing_engine/bronze/generic_bronze_loader.py" "${SPARK_HDFS_PATH}/"

# Deploy framework modules (if needed as --py-files)
hdfs dfs -put -f "${PROJECT_ROOT}/processing_engine/framework/"*.py "${SPARK_HDFS_PATH}/" 2>/dev/null || true

echo "✓ Spark jobs deployed to ${SPARK_HDFS_PATH}"
hdfs dfs -ls "${SPARK_HDFS_PATH}/"
