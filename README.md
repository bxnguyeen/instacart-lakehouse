# Instacart Lakehouse

A local learning project for batch data processing with MinIO,
Apache Iceberg, Nessie, Trino, dbt, Spark, and Airflow.

Instacart is a static dataset. The prepared batches simulate incremental
arrivals; they are not real daily data feeds.

## Architecture

```text
CSV files → MinIO landing → Spark → Bronze Iceberg tables
                                       ↓
                                  dbt Silver
                                       ↓
                                  dbt Gold

Airflow coordinates the pipeline.
Nessie manages the Iceberg catalog.
Trino queries the tables and executes dbt transformations.
MinIO stores landing files and Iceberg data.
```

## Pipeline

The active DAGs are:

- `instacart_bronze`
- `instacart_silver`
- `instacart_gold`

Transaction models process prepared batches using incremental merges.

Catalog data consists of aisles, departments, and products.
Silver and Gold record catalog processing state in MinIO.
When the catalog version is unchanged and marked as completed,
the pipeline validates existing tables and skips catalog rebuilding.

Set `refresh_catalog=true` to rebuild the catalog.

If validation fails on the skip path, the DAG stops.
Rerun with `refresh_catalog=true` after checking the failure.

## Repository Structure

```text
dags/       Airflow DAGs and catalog state helpers
dbt/        Models, macros, and data tests
infra/      Docker images and service configuration
scripts/    Batch preparation and validation
spark/      Bronze ingestion jobs
```

## Local Configuration

The following local files are excluded from Git:

- `infra/airflow/.env`
- `infra/airflow/config/airflow.cfg`
- `infra/trino/catalog/iceberg.properties`
- `infra/dbt/profiles.yml`
- `dbt/profiles.yml`

Source CSV files and prepared batches under `data/` are also excluded.

A fresh clone requires these configuration files and datasets.

## Setup

See [SETUP.md](SETUP.md) for local configuration, dataset preparation,
service startup, and pipeline execution.

## Build Pipeline Images

Run from the repository root:

```bash
docker build -f infra/Dockerfile.dbt -t instacart-dbt:local .
docker build -f infra/Dockerfile.spark -t instacart-spark:local .
```

## Validate dbt Configuration

With the local Python environment and dbt dependencies available:

```bash
cd dbt
dbt parse --profiles-dir .
```

This checks project parsing; it does not execute models or data tests.

## Run the Pipeline

With the local services running, trigger `instacart_bronze`
in Airflow.

Choose a prepared batch and set:

- `run_silver=true`
- `run_gold=true`
- `refresh_catalog=false`

The pipeline passes the catalog version to downstream DAGs.

## Current Validation Status

- Silver catalog build and skip paths were verified.
- Gold catalog build and skip paths were verified.
- Legacy snapshot DAGs and models were removed.
- dbt parsing passed after cleanup.
- dbt and Spark images were rebuilt after cleanup.
- After cleanup, batches 001, 002, and 003 completed successfully through
  Bronze, Silver, and Gold from a fresh clone with new storage and
  Airflow database volumes on the same macOS machine.
- Setup on a separate machine has not yet been verified.

## Fresh-Clone Validation

The pipeline was tested from a fresh GitHub clone on the same macOS
machine, using newly created MinIO, Nessie, and Airflow database volumes.

Batches 001, 002, and 003 completed successfully through Bronze, Silver,
and Gold. Catalog reuse was verified for subsequent batches.

| Batch | Bronze | Silver | Gold | Sum of DAG durations |
|---|---:|---:|---:|---:|
| 001 | 1:33 | 3:58 | 5:29 | 11:00 |
| 002 | 1:01 | 2:38 | 3:34 | 7:13 |
| 003 | 1:02 | 2:32 | 3:51 | 7:25 |

Validation identified and resolved missing bucket initialization,
missing forwarding of `run_gold`, and Docker command argument types.

Setup on a separate machine has not yet been verified.

## Current Merge Configuration

Silver prior items, Gold order items, and Gold orders use 8 merge
buckets. Bucket tasks run sequentially.

Bucket-specific uniqueness tests use a separate 8-way hash split;
their grouping is independent of the merge buckets.

## Validation with 8 Merge Buckets

| Batch | Bronze | Silver | Gold | Sum of DAG durations |
|---|---:|---:|---:|---:|
| 002 | 1:04 | 1:31 | 2:09 | 4:44 |
| 001 | 1:26 | 2:36 | 3:03 | 7:05 |

These measurements are reruns in the original local environment.
They are not directly comparable to the earlier fresh-storage runs.

Both reruns completed successfully through all three layers,
including the selected data tests.

## Scope

This project demonstrates local batch ingestion, incremental
transformations, data tests, and catalog reuse.

It is a learning project, not a production deployment.