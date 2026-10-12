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
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest-key', required=True)
    parser.add_argument('--table', choices=HEADERS, required=True)
    parser.add_argument("--skip-existing", action="store_true")
    args = parser.parse_args()
    endpoint = os.environ['MINIO_ENDPOINT']
    s3 = boto3.client('s3', endpoint_url=endpoint,
        aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
        aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
        region_name='us-east-1', config=Config(s3={'addressing_style': 'path'}))
    manifest = json.loads(s3.get_object(Bucket='landing', Key=args.manifest_key)['Body'].read())
    if manifest['bucket'] != 'landing':
        raise ValueError('Unexpected manifest bucket')
    matches = [r for r in manifest['files'] if r['name'] == args.table + '.csv']
    if len(matches) != 1:
        raise ValueError('Manifest must contain exactly one matching CSV')
    record = matches[0]
    prefix = args.manifest_key.rsplit('/', 1)[0]
    if record['key'] != prefix + '/' + record['name']:
        raise ValueError('CSV is outside the manifest snapshot')
    snapshot = prefix.rsplit('/', 1)[-1]
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
                .withColumn('_catalog_version', F.lit(snapshot))
                .withColumn('_source_file', F.lit('s3://landing/' + record['key']))
                .withColumn('_ingested_at', F.current_timestamp()))
            spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze LOCATION 's3://warehouse/bronze/'")
            table = 'lakehouse.bronze.' + args.table.replace('__', '_')

            if args.skip_existing and spark.catalog.tableExists(table):
                existing = spark.table(table).filter(
                    F.col("_catalog_version") == F.lit(snapshot)
                )

                expected_count = frame.count()
                existing_count = existing.count()

                wrong_source = (
                    existing.filter(
                        ~F.col("_source_file").eqNullSafe(
                            F.lit("s3://landing/" + record["key"])
                        )
                    )
                    .limit(1)
                    .count()
                )

                if (
                    expected_count > 0
                    and existing_count == expected_count
                    and wrong_source == 0
                ):
                    print(
                        f"BRONZE_SKIPPED table={table} "
                        f"version={snapshot} rows={existing_count}",
                        flush=True,
                    )
                    return

            if spark.catalog.tableExists(table):
                frame.writeTo(table).overwrite(F.col('_catalog_version') == F.lit(snapshot))
            else:
                (frame.writeTo(table).using('iceberg')
                    .tableProperty('format-version', '2')
                    .partitionedBy('_catalog_version').create())
            count = spark.table(table).filter(F.col('_catalog_version') == snapshot).count()
            print(f'BRONZE_SUCCESS table={table} snapshot={snapshot} rows={count}', flush=True)
        finally:
            spark.stop()


if __name__ == '__main__':
    main()
