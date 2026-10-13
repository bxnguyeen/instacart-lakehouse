# Instacart Lakehouse

A local batch lakehouse built with MinIO, Apache Iceberg, Nessie, Spark, Trino, dbt, and Airflow.

This branch, `perf/dbt-layer-build`, experiments with layer-level dbt builds instead of separate merge-bucket tasks.

Instacart is a static dataset. Prepared batches simulate incremental arrivals; they are not real daily data feeds.

## Architecture

```text
Source CSV files
      ↓
MinIO landing: CSV files and manifests
      ↓ Spark
Bronze Iceberg tables
      ↓ dbt SQL executed by Trino
Silver tables
      ↓ dbt SQL executed by Trino
Gold tables
```

| Component | Responsibility |
|---|---|
| MinIO | Stores landing files, Iceberg data, and metadata |
| Iceberg | Provides table schemas, snapshots, and file management |
| Nessie | Provides the Iceberg catalog |
| Spark | Reads source CSV files and writes Bronze tables |
| Trino | Queries Iceberg tables and executes dbt SQL |
| dbt | Defines transformations, model dependencies, and data tests |
| Airflow | Coordinates ingestion and downstream builds, logs, and retries |

## Data Preparation

Source files are placed under `data/instacart/`:

```text
aisles.csv
departments.csv
products.csv
orders.csv
order_products__prior.csv
order_products__train.csv
```

`prepare_batches.py` splits transaction data using `order_id % 10`:

- Remainders `0–7`: batch `001`.
- Remainder `8`: batch `002`.
- Remainder `9`: batch `003`.

Orders and their items use the same assignment rule.

`validate_batches.py` checks batch assignment, order uniqueness, item-to-order references, and row totals against the source files.

These ingestion batches are not machine-learning train/validation/test splits.

## Pipeline

The active DAGs are:

```text
instacart_bronze → instacart_silver → instacart_gold
```

Downstream DAGs are triggered asynchronously. Bronze success alone does not confirm that Silver and Gold completed successfully.

### Bronze

The Bronze DAG:

- Ensures the `landing` and `warehouse` buckets exist.
- Inspects source files and calculates checksums.
- Publishes catalog and batch files with manifests.
- Runs Spark jobs to load Bronze Iceberg tables.
- Passes batch and catalog information to Silver.

Bronze retains source-oriented data and adds ingestion metadata for traceability.

### Silver

Silver standardizes source fields and accumulates transaction data.

After the catalog is ready, one task, `build_silver_transactions`, runs `dbt build` for:

- `stg_orders_incremental`
- `stg_order_products_prior_incremental`
- `stg_order_products_train_incremental`
- Their selected data tests and the order-items overlap test

Transaction merges process the selected batch without merge-bucket filters.

### Gold

Gold organizes data into product dimensions and order facts.

| Model | Row grain |
|---|---|
| `dim_products` | One product |
| `fct_order_items_incremental` | One order–product pair |
| `fct_orders_incremental` | One order |

After the catalog is ready, one task, `build_gold_transactions`, builds the transaction models and runs their selected tests.

dbt uses model dependencies to build order items before order-level aggregations.

## Catalog Reuse

Catalog data consists of aisles, departments, and products.

Silver and Gold maintain separate state objects in MinIO:

```text
landing/instacart/state/silver_catalog.json
landing/instacart/state/gold_catalog.json
```

Before rebuilding, the corresponding layer records `building`. It records `completed` only after the required build, tests, and validation succeed.

With an unchanged completed catalog version and `refresh_catalog=false`, the pipeline validates existing catalog tables and skips rebuilding.

Transaction processing still runs.

If validation fails on the reuse path, the DAG stops. Inspect the failure before rerunning with `refresh_catalog=true`.

## Execution Strategy

This branch removes the Airflow merge-bucket loops and the corresponding SQL bucket filters and destination predicates.

- Each layer uses one dbt invocation for transaction builds and tests.
- dbt threads remain set to `1`.
- Airflow still controls layer execution and catalog handling.
- Retrying a transaction task reruns its selected dbt build, rather than an individual bucket.

Bucket-specific uniqueness tests retain their independent 8-way hash split. Test grouping does not control how transaction data is merged.

## Repository Structure

```text
dags/       Airflow DAGs and catalog state helpers
dbt/        Models, macros, and data tests
infra/      Docker images and service configuration
scripts/    Batch preparation and validation
spark/      Bronze ingestion jobs
README.md   Project overview
SETUP.md    Local setup instructions
```

## Setup

See [SETUP.md](SETUP.md) for configuration, dataset preparation, image builds, service startup, and pipeline execution.

Local configuration files are excluded from Git:

- `infra/airflow/.env`
- `infra/airflow/config/airflow.cfg`
- `infra/trino/catalog/iceberg.properties`
- `infra/dbt/profiles.yml`
- `dbt/profiles.yml`

Configuration examples are included. Source files and generated batches under `data/` are excluded.

A fresh clone requires local configuration and the source dataset before execution.

## Local Service Scripts

Run commands from the repository root.

### Check Configuration

```bash
bash scripts/check_setup.sh
```

Checks required configuration files, Docker availability, and Compose
configuration. It does not verify service readiness or credential matching.

### Build Images

```bash
bash scripts/build_images.sh
```

Builds dbt, Spark, MinIO, and Airflow images using Docker's build cache.
The script stops if a build fails.

Rebuild after changing dependencies, Dockerfiles, or code copied into
images. DAG-only changes do not require rebuilding.

### Start Services

```bash
bash scripts/start_services.sh
```

Starts the lakehouse, initializes Airflow, and starts Airflow services.
It does not rebuild images or trigger DAGs.

Check readiness before running the pipeline:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

Services with healthchecks should report `healthy`.

### Stop Services

Wait for active DAG runs to finish, then run:

```bash
bash scripts/stop_services.sh
```

Stops services while preserving data volumes. Use `start_services.sh`
to resume.

### Test Volume Overrides

To use separate test volumes, pass the lakehouse and Airflow override
files, in that order:

```bash
python3 scripts/init_config.py --test-name fresh-test-v3

bash scripts/start_services.sh \
  infra/generated-test-volumes.yaml \
  infra/airflow/generated-test-volumes.yaml

bash scripts/stop_services.sh \
  infra/generated-test-volumes.yaml \
  infra/airflow/generated-test-volumes.yaml
```

These files must exist locally and define the intended test volumes.
Choose unused volume names when testing from empty storage.

The current environments share Compose project names and host ports.
Run only one environment at a time. Separate volumes preserve data,
but do not isolate running containers.

The check, build, start, and stop scripts were exercised locally.
Startup readiness is checked manually.

## Validate dbt Configuration

With dbt installed in the local Python environment:

```bash
cd dbt
dbt parse --profiles-dir .
```

Parsing checks project configuration and references. It does not execute transformations or data tests.

## Run the Pipeline

With the services running, enable all three DAGs and trigger `instacart_bronze`:

```text
batch_id: 001
run_silver: true
run_gold: true
refresh_catalog: false
```

Wait for Bronze, Silver, and Gold to finish before processing the next batch.

## Fresh-Clone Validation

The layer-level build branch was tested from a fresh GitHub clone on the same macOS machine, using new MinIO, Nessie, and Airflow database volumes.

The reported runs completed successfully through all three layers with their selected tests.

| Batch | Bronze | Silver | Gold | Sum of DAG durations |
|---|---:|---:|---:|---:|
| `001` | 1:30 | 0:47 | 0:35 | **2:52** |
| `002` | 0:58 | 0:40 | 0:24 | **2:02** |
| `003` | 1:00 | 0:41 | 0:23 | **2:04** |

Batch `001` exercised initial table creation. Batches `002` and `003` exercised incremental processing with accumulated target data and catalog reuse.

The totals above are sums of DAG durations, not independently measured end-to-end wall-clock times.

Earlier bucket-based runs took longer in the recorded local measurements. Those runs had different environment and data states, so they are not controlled performance benchmarks.

## Limitations

- Setup on a separate machine has not yet been verified.
- Successful local runs do not establish performance at larger scales.
- Whole-batch merges may require more memory as data grows.
- Layer-level tasks provide fewer Airflow retry checkpoints than bucket-level tasks.
- No ML training or model deployment is implemented.

This is a learning project, not a production deployment.