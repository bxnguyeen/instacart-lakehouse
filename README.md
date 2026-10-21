# Instacart Lakehouse

A local ELT learning project using MinIO, Apache Iceberg, Nessie, Spark,
Trino, dbt, and Airflow.

One Airflow DAG, `instacart_pipeline`, organizes work into three stages.
Each processing task loads or builds one table. Stage-level tests check
consistency across tables.

## Architecture

```text
Local CSV files
  → MinIO landing
  → Spark → Iceberg tables in 01_raw
  → dbt / Trino → Iceberg tables in 02_stg
  → dbt / Trino → Iceberg tables in 03_dwh
```

MinIO stores files. Iceberg manages table metadata and snapshots. Nessie
provides the catalog. Spark ingests CSV files, and Trino executes SQL
submitted by dbt. Airflow coordinates tasks and records their status.
DBeaver can query Trino; it does not store the tables.

## DAG Structure

```text
raw: six per-table ingestion tasks
  → stg: six per-table dbt build tasks
  → test_stg_consistency
  → dwh: three per-table dbt build tasks
  → test_dwh_consistency
```

All raw tasks complete before staging begins. All staging tasks and
stage consistency tests complete before the warehouse stage starts.
Within DWH, `fct_orders` depends on `fct_order_items`.

Task concurrency and dbt threads are currently set to 1. Each table task
has a separate log and can be cleared and rerun in Airflow. Rerunning an
upstream table alone does not automatically refresh already completed
downstream tasks; include affected downstream tasks when needed.

## Tables

| Schema | Tables | Write behavior |
|---|---|---|
| `01_raw` | aisles, departments, products | Overwrite the full catalog table |
| `01_raw` | orders, order_products_prior, order_products_train | Overwrite the selected batch partition |
| `02_stg` | stg_aisles, stg_departments, stg_products | Rebuild catalog models |
| `02_stg` | stg_orders, stg_order_products_prior, stg_order_products_train | Incremental merge |
| `03_dwh` | dim_products | Rebuild product dimension |
| `03_dwh` | fct_order_items, fct_orders | Incremental merge |

`fct_order_items` has one row per `(order_id, product_id)`.
`fct_orders` has one row per `order_id`. `dim_products` has one row per product.

Raw catalog tables retain source columns. Raw transaction tables add
`_batch_id`. There are no custom checksum manifests, catalog-version
markers, or catalog-skip branches. Iceberg manages table snapshots;
rerun behavior is implemented by partition overwrite and merge logic.
Snapshots do not independently prevent duplicate business records.

## Simulated Batches

Instacart is static. `prepare_batches.py` assigns transactions using
`order_id % 10`: remainders 0–7 to `001`, 8 to `002`, and 9 to `003`.
Orders and their items follow the same assignment rule. These are simulated
ingestion batches, not daily feeds or ML dataset splits.

`validate_batches.py` remains an optional input-preparation check outside
the DAG. Batch preparation is required before the first run, but not
before every rerun when the source files are unchanged.

On initial creation or full refresh, incremental models can read all
available upstream data. On incremental execution, they filter the
selected `_batch_id` and merge by the configured business key.
This design assumes stable batch membership; it does not implement
propagation of source deletions or movement of records between batches.

## Data Quality

Per-table tasks use `dbt build` with cautious indirect test selection.
Selected single-model tests run with their models. After staging,
relationship tests and the prior/train overlap test run explicitly.
Warehouse consistency tests compare row counts, item totals, and metrics.
Uniqueness checks cover complete tables rather than hash buckets.

A failed test does not roll back tables already written. A successful
DAG means its selected checks passed, not that every possible business
rule was verified.

## Setup and Operation

See [SETUP.md](SETUP.md) for a fresh environment.

```bash
python3 scripts/init_config.py
bash scripts/check_setup.sh
bash scripts/build_images.sh
bash scripts/start_services.sh
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

After services are ready, enable `instacart_pipeline` and trigger with
`batch_id=001`. Continue with `002` after the run succeeds.

```bash
bash scripts/stop_services.sh
```

Stop only after active DAG runs finish. Stop preserves volumes.
Startup does not build images or wait for service healthchecks.

Rebuild images after changing dbt models/macros/tests or Spark code.
DAG-only changes are mounted directly and do not require image rebuilding.

## Repository Layout

```text
dags/       Pipeline orchestration
spark/      Raw ingestion
dbt/       Models, macros, and data tests
infra/      Dockerfiles, Compose, and configuration examples
scripts/    Configuration, lifecycle, and data preparation tools
```

Secrets, datasets, logs, dbt outputs, and generated volume overrides are
excluded from Git. Configuration examples are included.

## Validation and Scope

The single-DAG rebuild has a user-reported successful batch `001` run.
The numbered schema configuration was also reported to run successfully.
Earlier three-DAG and bucket-based timings are historical and are not
benchmarks for this architecture. Incremental processing and rerun
behavior should be rechecked on the final clean-storage configuration.

Validation has been local to macOS. A separate-machine or Ubuntu run has
not been verified. This is a learning project, not a production deployment.
