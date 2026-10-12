"""Load one Instacart CSV from a published manifest into Bronze Iceberg."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import tempfile

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession, functions as F, types as T

HEADERS = {
    'aisles': ['aisle_id', 'aisle'],
    'departments': ['department_id', 'department'],
    'products': ['product_id', 'product_name', 'aisle_id', 'department_id'],
    'orders': ['order_id', 'user_id', 'eval_set', 'order_number', 'order_dow', 'order_hour_of_day', 'days_since_prior_order'],
    'order_products__prior': ['order_id', 'product_id', 'add_to_cart_order', 'reordered'],
    'order_products__train': ['order_id', 'product_id', 'add_to_cart_order', 'reordered'],
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest-key', required=True)
    parser.add_argument('--table', choices=HEADERS, required=True)
    args = parser.parse_args()
    endpoint = os.environ['MINIO_ENDPOINT']
    s3 = boto3.client('s3', endpoint_url=endpoint,
        aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
        aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
        region_name='us-east-1', config=Config(s3={'addressing_style': 'path'}))
    
    response = s3.get_object(
        Bucket="landing",
        Key=args.manifest_key,
    )
    body = response["Body"]

    try:
        manifest_bytes = body.read()
    finally:
        body.close()

    manifest = json.loads(manifest_bytes)
    manifest_id = hashlib.sha256(manifest_bytes).hexdigest()

    if manifest.get("version") != 1:
        raise ValueError("Unsupported manifest version")

    if manifest.get("bucket") != "landing":
        raise ValueError("Unexpected manifest bucket")

    batch_id = manifest.get("batch_id")

    if batch_id not in {"001", "002", "003"}:
        raise ValueError(f"Unsupported batch_id: {batch_id}")

    expected_manifest_key = (
        f"instacart/batches/{batch_id}/"
        f"manifests/{manifest_id}.json"
    )

    if args.manifest_key != expected_manifest_key:
        raise ValueError("Manifest content does not match its key")

    allowed_tables = {
        "orders",
        "order_products__prior",
        "order_products__train",
    }

    if args.table not in allowed_tables:
        raise ValueError(f"Unsupported batch table: {args.table}")

    expected_files = {name + ".csv" for name in allowed_tables}
    records = manifest["files"]

    if (
        len(records) != len(expected_files)
        or {record["name"] for record in records} != expected_files
    ):
        raise ValueError("Manifest must contain all three batch files")

    record = next(
        record
        for record in records
        if record["name"] == args.table + ".csv"
    )

    expected_file_key = (
        f"instacart/batches/{batch_id}/"
        f"{record['sha256']}/{record['name']}"
    )

    if record["key"] != expected_file_key:
        raise ValueError("CSV key does not match the batch manifest")
    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / record['name']
        s3.download_file('landing', record['key'], str(path))
        digest = hashlib.sha256()
        with path.open('rb') as file:
            for chunk in iter(lambda: file.read(8 * 1024 * 1024), b''):
                digest.update(chunk)
        if path.stat().st_size != record['size'] or digest.hexdigest() != record['sha256']:
            raise ValueError('Downloaded CSV does not match manifest')
        with path.open(newline='', encoding='utf-8-sig') as file:
            if next(csv.reader(file)) != HEADERS[args.table]:
                raise ValueError('Unexpected CSV header')
        builder = SparkSession.builder.appName('instacart-bronze-' + args.table)
        settings = {
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.lakehouse': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.lakehouse.catalog-impl': 'org.apache.iceberg.nessie.NessieCatalog',
            'spark.sql.catalog.lakehouse.uri': 'http://nessie:19120/api/v2',
            'spark.sql.catalog.lakehouse.ref': 'main',
            'spark.sql.catalog.lakehouse.authentication.type': 'NONE',
            'spark.sql.catalog.lakehouse.warehouse': 's3://warehouse/',
            'spark.sql.catalog.lakehouse.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
            'spark.sql.catalog.lakehouse.s3.endpoint': endpoint,
            'spark.sql.catalog.lakehouse.s3.path-style-access': 'true',
            'spark.sql.catalog.lakehouse.client.region': 'us-east-1',
            'spark.sql.shuffle.partitions': '2',
            'spark.sql.session.timeZone': 'UTC',
        }
        for key, value in settings.items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()
        try:
            spark.sparkContext.setLogLevel('WARN')
            schema = T.StructType([T.StructField(c, T.StringType(), True) for c in HEADERS[args.table]])
            frame = (spark.read.option('header', True).option('mode', 'FAILFAST')
                .option('escape', '"').schema(schema).csv(str(path))
                .withColumn("_batch_id", F.lit(batch_id))
                .withColumn("_manifest_id", F.lit(manifest_id))
                .withColumn('_source_file', F.lit('s3://landing/' + record['key']))
                .withColumn('_ingested_at', F.current_timestamp()))
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze LOCATION 's3://warehouse/bronze/'")
            table = (
                "lakehouse.bronze."
                + args.table.replace("__", "_")
                + "_batches"
            )

            if spark.catalog.tableExists(table):
                frame.writeTo(table).overwrite(
                    F.col("_batch_id") == F.lit(batch_id)
                )
            else:
                (
                    frame.writeTo(table)
                    .using("iceberg")
                    .tableProperty("format-version", "2")
                    .partitionedBy("_batch_id")
                    .create()
                )

            count = (
                spark.table(table)
                .filter(F.col("_batch_id") == F.lit(batch_id))
                .count()
            )

            print(
                f"BRONZE_BATCH_SUCCESS table={table} "
                f"batch={batch_id} manifest={manifest_id} rows={count}",
                flush=True,
            )
        finally:
            spark.stop()


if __name__ == '__main__':
    main()
