# Local Setup

These instructions describe setup from a fresh clone.

The pipeline was validated after repository cleanup using a fresh clone
and new MinIO, Nessie, and Airflow database volumes on the same macOS
machine. Setup on a separate machine has not yet been verified.

## Requirements

- Docker with Compose and a running Docker engine.
- Python 3 for dataset preparation.
- Git.
- Sufficient memory and disk space for Spark, Trino, Airflow, and the dataset.

The current configuration uses a Unix Docker socket. macOS with Docker
Desktop and Linux are the intended environments. Native Windows setup
has not been validated; use a Linux environment such as WSL2.

Run all commands below from the repository root unless stated otherwise.

## 1. Create local configuration

```bash
cp infra/airflow/.env.example infra/airflow/.env
cp infra/dbt/profiles.example.yml infra/dbt/profiles.yml
cp dbt/profiles.example.yml dbt/profiles.yml
cp infra/trino/catalog/iceberg.properties.example infra/trino/catalog/iceberg.properties
mkdir -p infra/airflow/config logs/airflow plugins data/instacart data/batches
```

Edit `infra/airflow/.env`:

- Keep `MINIO_ACCESS_KEY=lakehouse` to match the current Compose configuration.
- Set `MINIO_SECRET_KEY` to a local password of at least eight characters.
- Generate a new Fernet key and JWT secret for this installation.
- Set a local Airflow username and password.
- On Linux, set `AIRFLOW_UID` to the output of `id -u`.

Generate the keys with Python's standard library:

```bash
python3 - <<'PY'
import base64
import secrets
print('FERNET_KEY=' + base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())
print('AIRFLOW__API_AUTH__JWT_SECRET=' + secrets.token_hex(32))
PY
```

Copy the generated values into `.env`. Do not commit or share them.

Edit `infra/trino/catalog/iceberg.properties` and replace
`REPLACE_WITH_MINIO_SECRET_KEY` with the same MinIO password.

Do not replace keys or credentials in an existing installation as part
of ordinary repository setup. These generation steps are for a fresh installation.

## 2. Prepare the dataset

Obtain the Instacart Market Basket Analysis dataset from its original
provider, following its access terms. Extract these files into
`data/instacart/`:

```text
aisles.csv
departments.csv
products.csv
orders.csv
order_products__prior.csv
order_products__train.csv
```

Prepare and validate batches:

```bash
python3 scripts/prepare_batches.py
python3 scripts/validate_batches.py
```

Batches `001`, `002`, and `003` simulate incremental arrivals using a
deterministic split by order ID. They are not real daily source feeds.
Do not run batch preparation concurrently with ingestion.

## 3. Validate configuration and build images

```bash
docker info
docker compose --env-file infra/airflow/.env -f infra/compose.yaml config --quiet
docker compose --env-file infra/airflow/.env -f infra/airflow/compose.yaml config --quiet
docker build -f infra/Dockerfile.dbt -t instacart-dbt:local .
docker build -f infra/Dockerfile.spark -t instacart-spark:local .
docker compose --env-file infra/airflow/.env -f infra/compose.yaml build minio
docker compose --env-file infra/airflow/.env -f infra/airflow/compose.yaml build
```

The dbt Dockerfile requires `infra/dbt/profiles.yml` before building.
Container dbt connects to `trino:8080`; host dbt connects to `localhost:8081`.

## 4. Start services

Start the lakehouse first; this creates the external network Airflow uses:

```bash
docker compose --env-file infra/airflow/.env -f infra/compose.yaml up -d
```

Initialize Airflow and inspect the result before starting its services:

```bash
docker compose --env-file infra/airflow/.env -f infra/airflow/compose.yaml up airflow-init
```

Continue only if `airflow-init` exits successfully:

```bash
docker compose --env-file infra/airflow/.env -f infra/airflow/compose.yaml up -d airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer
```

Inspect service state:

```bash
docker compose --env-file infra/airflow/.env -f infra/compose.yaml ps
docker compose --env-file infra/airflow/.env -f infra/airflow/compose.yaml ps
```

A successful image build does not prove service readiness. If initialization
or startup fails, inspect service logs before triggering the pipeline.

## 5. Run the pipeline

- Airflow: http://localhost:8080
- MinIO console: http://localhost:9001
- Trino: http://localhost:8081

Log into Airflow using the local credentials configured above.
Trigger `instacart_bronze` with batch `001`, `run_silver=true`,
`run_gold=true`, and `refresh_catalog=false`.

Bronze passes the catalog version to Silver, and Silver passes it to Gold.
Downstream DAGs run asynchronously: Bronze success does not prove Silver
or Gold success. Inspect all three DAG runs and their tests.

Process batch `002` and then `003` after the preceding downstream runs
complete. Avoid overlapping runs across layers when changing catalogs.

Catalog completion state is stored in MinIO under:

```text
landing/instacart/state/silver_catalog.json
landing/instacart/state/gold_catalog.json
```

With the same completed catalog version, Silver and Gold validate existing
tables and skip catalog rebuilding. Transaction tasks still execute.
If validation fails, inspect the error and rerun with
`refresh_catalog=true` when rebuilding is appropriate.

## 6. Stop and resume

Wait for active pipeline runs to finish before stopping services.

```bash
docker compose --env-file infra/airflow/.env -f infra/airflow/compose.yaml stop
docker compose --env-file infra/airflow/.env -f infra/compose.yaml stop
```

These commands preserve data volumes. Resume with the lakehouse `up -d`
command followed by the Airflow service `up -d` command from step 4.
Airflow initialization is not required for every restart.

## Validation Status

Batches 001, 002, and 003 completed successfully through Bronze,
Silver, and Gold in the fresh-clone environment.

Catalog build and reuse paths were verified. Fresh-clone testing
identified and resolved missing bucket initialization, missing
forwarding of `run_gold`, and Docker command argument types.

Validation used separate test volumes on the same macOS machine.
Setup on a separate machine has not yet been verified.


## Service Script Shortcuts

From the repository root, use:

```bash
bash scripts/check_setup.sh
bash scripts/build_images.sh
bash scripts/start_services.sh
# Wait for active DAG runs to finish before stopping services.
bash scripts/stop_services.sh
```

For the included test-volume configuration, pass both override files:

```bash
bash scripts/start_services.sh infra/layer-test-volumes.yaml infra/airflow/layer-test-volumes.yaml
bash scripts/stop_services.sh infra/layer-test-volumes.yaml infra/airflow/layer-test-volumes.yaml
```

Check that test volume names are unused before a fresh-storage test.
Startup does not build images or wait for healthchecks. Verify readiness
before triggering DAGs. Environments share container names and ports;
run only one at a time.
