# Local Setup

Run commands from the repository root unless stated otherwise.

## 1. Prerequisites

Install Git, Python 3, Bash, and Docker with Compose. Ensure the Docker
engine is running. Airflow, dbt, Spark, Trino, MinIO, Nessie, and PostgreSQL
run in containers and do not need separate host installations.

The configuration assumes a Unix Docker socket. macOS is the locally
tested environment; Ubuntu portability has not yet been verified.

```bash
git clone https://github.com/bxnguyeen/instacart-lakehouse.git
cd instacart-lakehouse
docker info
```

While the rebuild remains on a branch, select it before setup:

```bash
git switch refactor/single-dag-incremental
```

This requires that branch to have been pushed. Omit the switch once the
architecture is merged into the repository's default branch.

## 2. Initialize Configuration

```bash
python3 scripts/init_config.py
```

The script generates missing local credentials and copies configuration
examples. It preserves existing files and checks that MinIO credentials
match between `.env` and the Trino Iceberg configuration.

Local files include:

- `infra/airflow/.env`
- `infra/dbt/profiles.yml` for container dbt at `trino:8080`
- `dbt/profiles.yml` for optional host dbt at `localhost:8081`
- `infra/trino/catalog/iceberg.properties`

Keep these files private. Airflow login credentials are in the local
`.env`. Existing placeholder values must be completed manually. Do not
regenerate credentials when resuming an existing environment.
On Linux, verify `AIRFLOW_UID` matches `id -u`; new configuration generation
sets it automatically.

## 3. Prepare Source Data

Obtain the Instacart dataset under its provider's access terms. Put these
files in `data/instacart/`:

```text
aisles.csv
departments.csv
products.csv
orders.csv
order_products__prior.csv
order_products__train.csv
```

```bash
python3 scripts/prepare_batches.py
# Optional verification of the prepared input:
python3 scripts/validate_batches.py
```

Prepare files before triggering the DAG. Do not regenerate batches while
ingestion is running. Existing unchanged, prepared batches can be reused.

## 4. Choose Storage

Without generated overrides, Compose uses its default named volumes.
To choose separate test storage before starting services:

```bash
python3 scripts/init_config.py --test-name rebuild-v1
```

This creates:

```text
infra/generated-test-volumes.yaml
infra/airflow/generated-test-volumes.yaml
```

Start and stop scripts automatically use both files when present. If only
one exists, they fail rather than mix storage configurations. Existing
override files are preserved; a new `--test-name` does not replace them.
Inspect the files to confirm the selected names.

Before a fresh-storage test:

```bash
docker volume ls --format '{{.Name}}' | grep '^instacart-rebuild-v1-'
```

No matching names means those volumes do not yet exist. Matches mean the
storage is not proven empty. For a different test name, stop the current
environment first, preserve its volume names, remove only the two generated
YAML files, and rerun initialization with an unused name. Removing YAML
does not delete Docker data.

Volumes accumulate disk usage. Fresh volumes are needed for clean-install
tests, not for every code change. Do not delete MinIO files directly while
retaining the corresponding catalog references.

## 5. Check and Build

```bash
bash scripts/check_setup.sh
bash scripts/build_images.sh
```

The configuration check verifies required files, Docker availability, and
Compose syntax. It does not verify dataset completeness or service readiness.
Build stops on failure and uses available Docker cache.

## 6. Start Services

```bash
bash scripts/start_services.sh
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

The script starts the lakehouse, runs Airflow initialization, then starts
Airflow services. Wait for services with healthchecks to report `healthy`.
MinIO and Nessie may show only `Up`; this alone is not a readiness probe.

- Airflow: http://localhost:8080
- MinIO console: http://localhost:9001
- Trino: http://localhost:8081

All environments currently share Compose project names, host ports, and
image tags. Run only one at a time. Separate volumes do not isolate running
containers or images. Check mounts when switching between clone directories:

```bash
docker inspect instacart-airflow-airflow-scheduler-1 \
  --format '{{range .Mounts}}{{if eq .Destination "/opt/airflow/dags"}}{{.Source}}{{end}}{{end}}'
```

It must point to the intended repository's `dags/` directory.

## 7. Execute the Pipeline

Enable `instacart_pipeline` in Airflow. Trigger with `batch_id=001`.
The DAG runs per-table tasks in `raw`, `stg`, and `dwh` groups plus the
stage consistency checks. Do not trigger legacy DAGs if an older checkout
still contains them.

Wait for every task and test to succeed, then run `002` and `003` in order.
Only `batch_id` is required; there are no `run_silver`, `run_gold`,
`catalog_version`, or `refresh_catalog` parameters.

Check tables through DBeaver using Trino at `localhost:8081`:

```sql
SHOW SCHEMAS FROM iceberg;
SELECT count(*) FROM iceberg."03_dwh".fct_orders;
SELECT count(*) FROM iceberg."03_dwh".fct_order_items;
SELECT count(*) FROM iceberg."03_dwh".dim_products;
```

Schemas are `01_raw`, `02_stg`, and `03_dwh`. Numbered identifiers need
quoting in SQL. Old schemas do not disappear after editing code.

To validate rerun behavior, record counts and business metrics, rerun the
same batch, and compare them. Count equality alone is insufficient; inspect
uniqueness and business tests as well.

## 8. Stop and Resume

Wait for active runs to finish:

```bash
bash scripts/stop_services.sh
```

This preserves data. Resume with `bash scripts/start_services.sh`, keeping
the same local configuration and volume overrides. No rebuild is needed
unless image contents changed.

## Development Checks

If dbt is installed in a host Python environment, parsing is optional:

```bash
cd dbt
dbt parse --profiles-dir . --no-partial-parse
```

Host dbt is not required to execute the pipeline. Parsing does not execute
SQL or data tests. Rebuild the dbt image after SQL/YAML/macro/test changes,
and the Spark image after loader changes. Editing the DAG does not require
an image rebuild. Resetting volumes does not update images.

## Validation Status

The user reported a successful batch `001` on the single-DAG rebuild and a
successful run after numbered schema changes. Final clean-storage
incremental and rerun checks remain separate checkpoints. Historical runs
of the earlier architecture are not validation of this exact version.
Separate-machine and Ubuntu setup have not yet been verified.
