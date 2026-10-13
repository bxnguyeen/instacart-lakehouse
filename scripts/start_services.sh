#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

if [[ $# -ne 0 && $# -ne 2 ]]; then
  echo "Usage: bash scripts/start_services.sh [lakehouse_override airflow_override]" >&2
  exit 1
fi

bash scripts/check_setup.sh

lakehouse=(
  docker compose
  --env-file infra/airflow/.env
  -f infra/compose.yaml
)

airflow=(
  docker compose
  --env-file infra/airflow/.env
  -f infra/airflow/compose.yaml
)

if [[ $# -eq 2 ]]; then
  for file in "$1" "$2"; do
    if [[ ! -f "$file" ]]; then
      echo "ERROR: Override file not found: $file" >&2
      exit 1
    fi
  done

  lakehouse+=(-f "$1")
  airflow+=(-f "$2")
fi

echo "Starting lakehouse services..."
"${lakehouse[@]}" up -d

echo "Initializing Airflow..."
"${airflow[@]}" run --rm airflow-init

echo "Starting Airflow services..."
"${airflow[@]}" up -d \
  airflow-apiserver \
  airflow-scheduler \
  airflow-dag-processor \
  airflow-triggerer

"${lakehouse[@]}" ps
"${airflow[@]}" ps

echo "Services started. Check health status before triggering DAGs."
