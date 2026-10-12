from pathlib import Path
import hashlib
import os
import json
from datetime import timedelta
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)

import logging
import pendulum
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from airflow.sdk import dag, task, Param, get_current_context


BATCH_ROOT = Path("/opt/airflow/data/batches")
FILES = [
    "orders.csv",
    "order_products__prior.csv",
    "order_products__train.csv",
]

CATALOG_ROOT = Path("/opt/airflow/data/instacart")
CATALOG_FILES = [
    "aisles.csv",
    "departments.csv",
    "products.csv",
]


def checksum(path):
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(8 * 1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()



@dag(
    dag_id="instacart_bronze",
    start_date=pendulum.datetime(
        2026, 10, 1, tz="Asia/Ho_Chi_Minh"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "batch_id": Param(
            "001",
            type="string",
            enum=["001", "002", "003"],
        ),
        "run_silver": Param(False, type="boolean"),
        "run_gold": Param(False, type="boolean"),
        "refresh_catalog": Param(False, type="boolean"),
    },
    render_template_as_native_obj=True,
    tags=["instacart", "incremental", "bronze"],
)
def instacart_bronze():
    @task
    def ensure_buckets():
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}),
        )

        for bucket in ["landing", "warehouse"]:
            try:
                s3.head_bucket(Bucket=bucket)
            except ClientError as error:
                code = error.response["Error"]["Code"]
                if code not in {"404", "NoSuchBucket", "NotFound"}:
                    raise

                s3.create_bucket(Bucket=bucket)
                logging.info("Created bucket: %s", bucket)
            else:
                logging.info("Bucket already exists: %s", bucket)

    @task
    def inspect_catalog():
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        refresh = conf.get(
            "refresh_catalog",
            context["params"]["refresh_catalog"],
        )

        if not isinstance(refresh, bool):
            raise ValueError("refresh_catalog must be a boolean")

        records = []

        for filename in sorted(CATALOG_FILES):
            path = CATALOG_ROOT / filename

            if not path.is_file():
                raise ValueError(f"Missing catalog file: {path}")

            size = path.stat().st_size

            if size == 0:
                raise ValueError(f"Empty catalog file: {path}")

            records.append({
                "name": filename,
                "size": size,
                "sha256": checksum(path),
            })

        version_input = json.dumps(
            records,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        catalog_version = hashlib.sha256(
            version_input
        ).hexdigest()

        logging.info(
            "Catalog inspected: version=%s files=%s refresh=%s",
            catalog_version,
            len(records),
            refresh,
        )

        return {
            "catalog_version": catalog_version,
            "refresh_catalog": refresh,
            "files": records,
        }

    @task
    def publish_catalog(catalog):
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}),
        )

        bucket = "landing"
        version = catalog["catalog_version"]
        prefix = f"instacart/catalog/{version}"
        uploaded = []

        s3.head_bucket(Bucket=bucket)

        for record in catalog["files"]:
            path = CATALOG_ROOT / record["name"]

            if (
                path.stat().st_size != record["size"]
                or checksum(path) != record["sha256"]
            ):
                raise ValueError(
                    f"Catalog file changed after inspection: {record['name']}"
                )

            key = f"{prefix}/{record['name']}"

            try:
                head = s3.head_object(Bucket=bucket, Key=key)
            except ClientError as error:
                code = error.response["Error"]["Code"]
                if code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
                head = None

            if head is None:
                s3.upload_file(
                    str(path),
                    bucket,
                    key,
                    ExtraArgs={
                        "Metadata": {"sha256": record["sha256"]}
                    },
                )
                head = s3.head_object(Bucket=bucket, Key=key)
                logging.info("Catalog uploaded: %s", key)
            else:
                logging.info("Catalog object already exists: %s", key)

            if (
                head["ContentLength"] != record["size"]
                or head.get("Metadata", {}).get("sha256")
                != record["sha256"]
            ):
                raise ValueError(f"Catalog object mismatch: {key}")

            uploaded.append({**record, "key": key})

        manifest = {
            "version": 1,
            "catalog_version": version,
            "bucket": bucket,
            "files": uploaded,
        }

        body = json.dumps(
            manifest,
            sort_keys=True,
            indent=2,
        ).encode("utf-8")

        manifest_key = f"{prefix}/manifest.json"

        s3.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=body,
            ContentType="application/json",
        )

        saved_body = s3.get_object(
            Bucket=bucket,
            Key=manifest_key,
        )["Body"]

        try:
            if saved_body.read() != body:
                raise ValueError("Catalog manifest verification failed")
        finally:
            saved_body.close()

        logging.info(
            "Catalog published: version=%s manifest=s3://%s/%s",
            version,
            bucket,
            manifest_key,
        )

        return {
            "catalog_version": version,
            "refresh_catalog": catalog["refresh_catalog"],
            "bucket": bucket,
            "manifest_key": manifest_key,
        }

    @task
    def inspect_batch():
        context = get_current_context()
        batch_id = context["params"]["batch_id"]

        if batch_id not in {"001", "002", "003"}:
            raise ValueError(f"Unsupported batch_id: {batch_id}")

        batch_dir = BATCH_ROOT / batch_id
        records = []

        for filename in FILES:
            path = batch_dir / filename

            if not path.is_file():
                raise ValueError(f"Missing input file: {path}")

            size = path.stat().st_size

            if size == 0:
                raise ValueError(f"Empty input file: {path}")

            records.append({
                "name": filename,
                "size": size,
                "sha256": checksum(path),
            })

            logging.info(
                "Input verified: batch=%s file=%s size_bytes=%s",
                batch_id,
                filename,
                size,
            )

        return {
            "batch_id": batch_id,
            "files": records,
        }

    @task
    def upload_batch(batch):
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}),
        )

        bucket = "landing"
        s3.head_bucket(Bucket=bucket)

        batch_id = batch["batch_id"]
        uploaded = []

        for record in batch["files"]:
            path = BATCH_ROOT / batch_id / record["name"]

            if (
                path.stat().st_size != record["size"]
                or checksum(path) != record["sha256"]
            ):
                raise ValueError(
                    f"Source changed after inspection: {record['name']}"
                )

            key = (
                f"instacart/batches/{batch_id}/"
                f"{record['sha256']}/{record['name']}"
            )

            try:
                head = s3.head_object(Bucket=bucket, Key=key)
            except ClientError as error:
                code = error.response["Error"]["Code"]
                if code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
                head = None

            if head is None:
                s3.upload_file(
                    str(path),
                    bucket,
                    key,
                    ExtraArgs={
                        "Metadata": {"sha256": record["sha256"]}
                    },
                )
                head = s3.head_object(Bucket=bucket, Key=key)
                logging.info("Uploaded: s3://%s/%s", bucket, key)
            else:
                logging.info(
                    "Object already exists: s3://%s/%s",
                    bucket,
                    key,
                )

            if (
                head["ContentLength"] != record["size"]
                or head.get("Metadata", {}).get("sha256")
                != record["sha256"]
            ):
                raise ValueError(
                    f"Object verification failed: {key}"
                )

            uploaded.append({**record, "key": key})

        return {
            "batch_id": batch_id,
            "bucket": bucket,
            "files": uploaded,
        }

    @task
    def publish_manifest(batch):
        records = sorted(
            batch["files"],
            key=lambda record: record["name"],
        )

        if (
            len(records) != len(FILES)
            or {record["name"] for record in records} != set(FILES)
        ):
            raise ValueError(
                "Batch must contain all three expected files"
            )

        manifest = {
            "version": 1,
            "batch_id": batch["batch_id"],
            "bucket": batch["bucket"],
            "files": records,
        }

        body = json.dumps(
            manifest,
            sort_keys=True,
            indent=2,
        ).encode("utf-8")

        manifest_id = hashlib.sha256(body).hexdigest()
        key = (
            f"instacart/batches/{batch['batch_id']}/"
            f"manifests/{manifest_id}.json"
        )

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}),
        )

        s3.put_object(
            Bucket=batch["bucket"],
            Key=key,
            Body=body,
            ContentType="application/json",
        )

        saved_body = s3.get_object(
            Bucket=batch["bucket"],
            Key=key,
        )["Body"]

        try:
            if saved_body.read() != body:
                raise ValueError(
                    f"Manifest verification failed: {key}"
                )
        finally:
            saved_body.close()

        logging.info(
            "Batch ready: batch=%s manifest=s3://%s/%s",
            batch["batch_id"],
            batch["bucket"],
            key,
        )

        return {
            "batch_id": batch["batch_id"],
            "bucket": batch["bucket"],
            "manifest_key": key,
        }

    buckets_ready = ensure_buckets()
    catalog = inspect_catalog()
    published_catalog = publish_catalog(catalog)

    buckets_ready >> published_catalog

    previous_catalog = published_catalog

    for table in ["aisles", "departments", "products"]:
        load_catalog = DockerOperator(
            task_id=f"load_catalog_{table}",
            image="instacart-spark:local",
            api_version="auto",
            docker_url="unix://var/run/docker.sock",
            network_mode="instacart-lakehouse_default",
            force_pull=False,
            mount_tmp_dir=False,
            environment={
                "MINIO_ENDPOINT": "http://minio:9000",
                "AWS_REGION": "us-east-1",
            },
            private_environment={
                "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
                "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
                "AWS_ACCESS_KEY_ID": os.environ["MINIO_ACCESS_KEY"],
                "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_SECRET_KEY"],
            },
            command=(
                "/opt/spark/bin/spark-submit "
                "--master local[2] "
                "--driver-memory 1g "
                "/opt/jobs/load_catalog.py "
                "--manifest-key "
                "{{ ti.xcom_pull(task_ids='publish_catalog')['manifest_key'] }} "
                "--table " + table + " "
                "{% if not ti.xcom_pull(task_ids='publish_catalog')"
                "['refresh_catalog'] %}"
                "--skip-existing"
                "{% endif %}"
            ),
            mem_limit="2g",
            auto_remove="force",
            do_xcom_push=False,
            retries=0,
            execution_timeout=timedelta(minutes=45),
        )

        previous_catalog >> load_catalog
        previous_catalog = load_catalog

    batch = inspect_batch()
    previous_catalog >> batch

    uploaded = upload_batch(batch)
    manifest = publish_manifest(uploaded)

    previous = manifest

    for table in [
        "orders",
        "order_products__prior",
        "order_products__train",
    ]:
        load = DockerOperator(
            task_id="load_" + table.replace("__", "_") + "_batch",
            image="instacart-spark:local",
            api_version="auto",
            docker_url="unix://var/run/docker.sock",
            network_mode="instacart-lakehouse_default",
            force_pull=False,
            mount_tmp_dir=False,
            environment={
                "MINIO_ENDPOINT": "http://minio:9000",
                "AWS_REGION": "us-east-1",
            },
            private_environment={
                "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
                "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
                "AWS_ACCESS_KEY_ID": os.environ["MINIO_ACCESS_KEY"],
                "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_SECRET_KEY"],
            },
            command=[
                "/opt/spark/bin/spark-submit",
                "--master", "local[2]",
                "--driver-memory", "1g",
                "/opt/jobs/load_bronze_batch.py",
                "--manifest-key",
                "{{ ti.xcom_pull(task_ids='publish_manifest')['manifest_key'] }}",
                "--table", table,
            ],
            mem_limit="2g",
            auto_remove="force",
            do_xcom_push=False,
            retries=0,
            execution_timeout=timedelta(minutes=45),
        )

        previous >> load
        previous = load

    @task.short_circuit
    def should_run_silver():
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        value = conf.get(
            "run_silver",
            context["params"]["run_silver"],
        )

        if not isinstance(value, bool):
            raise ValueError("run_silver must be a boolean")

        return value

    silver_gate = should_run_silver()

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="instacart_silver",
        trigger_run_id="bronze__{{ run_id }}",
        logical_date=None,
        conf={
            "batch_id": (
                "{{ ti.xcom_pull("
                "task_ids='publish_manifest')['batch_id'] }}"
            ),
            "catalog_version": (
                "{{ ti.xcom_pull("
                "task_ids='publish_catalog')['catalog_version'] }}"
            ),
            "refresh_catalog": (
                "{{ ti.xcom_pull("
                "task_ids='publish_catalog')['refresh_catalog'] }}"
            ),
            "run_gold": "{{ params.run_gold }}",
        },
        wait_for_completion=False,
        skip_when_already_exists=True,
    )

    previous >> silver_gate >> trigger_silver


instacart_bronze()