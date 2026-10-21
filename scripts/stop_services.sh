#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

if [[ $# -ne 0 && $# -ne 2 ]]; then
  echo "Usage: bash scripts/stop_services.sh [lakehouse_override airflow_override]" >&2
  exit 1
fi

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

if [[ $# -eq 0 ]]; then
  lakehouse_override="infra/generated-test-volumes.yaml"
  airflow_override="infra/airflow/generated-test-volumes.yaml"

  if [[ -f "$lakehouse_override" && -f "$airflow_override" ]]; then
    set -- "$lakehouse_override" "$airflow_override"
    echo "Using generated volume overrides."
  elif [[ -f "$lakehouse_override" || -f "$airflow_override" ]]; then
    echo "ERROR: Both generated override files must exist." >&2
    exit 1
  fi
fi

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

echo "Stopping Airflow services..."
"${airflow[@]}" stop

echo "Stopping lakehouse services..."
"${lakehouse[@]}" stop

echo "PASS: Services stopped. Data volumes preserved."
