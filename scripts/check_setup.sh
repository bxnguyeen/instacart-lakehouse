#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

required_files=(
  "infra/airflow/.env"
  "infra/dbt/profiles.yml"
  "infra/trino/catalog/iceberg.properties"
)

for file in "${required_files[@]}"; do
  if [[ ! -s "$file" ]]; then
    echo "ERROR: Missing or empty configuration file: $file" >&2
    exit 1
  fi
done

docker info >/dev/null

docker compose --env-file infra/airflow/.env \
  -f infra/compose.yaml config --quiet

docker compose --env-file infra/airflow/.env \
  -f infra/airflow/compose.yaml config --quiet

echo "PASS: Required configuration exists and Compose validation passed."
