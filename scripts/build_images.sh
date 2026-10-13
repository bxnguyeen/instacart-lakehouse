#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

bash scripts/check_setup.sh

echo "Building dbt image..."
docker build -f infra/Dockerfile.dbt -t instacart-dbt:local .

echo "Building Spark image..."
docker build -f infra/Dockerfile.spark -t instacart-spark:local .

echo "Building MinIO image..."
docker compose --env-file infra/airflow/.env \
  -f infra/compose.yaml build minio

echo "Building Airflow image..."
docker compose --env-file infra/airflow/.env \
  -f infra/airflow/compose.yaml build

echo "PASS: All pipeline images built successfully."
