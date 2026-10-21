"""Load one source CSV from MinIO into a raw Iceberg table."""
import argparse
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
    'orders': ['order_id', 'user_id', 'eval_set', 'order_number', 'order_dow',
               'order_hour_of_day', 'days_since_prior_order'],
    'order_products_prior': ['order_id', 'product_id', 'add_to_cart_order', 'reordered'],
    'order_products_train': ['order_id', 'product_id', 'add_to_cart_order', 'reordered'],
}
CATALOG_TABLES = {'aisles', 'departments', 'products'}
FILENAMES = {
    name: name.replace('order_products_', 'order_products__') + '.csv'
    for name in HEADERS
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table', required=True, choices=HEADERS)
    parser.add_argument('--batch-id', choices=['001', '002', '003'])
    args = parser.parse_args()
    is_transaction = args.table not in CATALOG_TABLES
    if is_transaction and args.batch_id is None:
        parser.error('--batch-id is required for transaction tables')

    endpoint = os.environ['MINIO_ENDPOINT']
    s3 = boto3.client(
        's3', endpoint_url=endpoint,
        aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
        aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
        region_name='us-east-1',
        config=Config(s3={'addressing_style': 'path'}),
    )
    prefix = (f'instacart/batches/{args.batch_id}' if is_transaction
              else 'instacart/catalog')
    key = f'{prefix}/{FILENAMES[args.table]}'

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
    builder = SparkSession.builder.appName(f'instacart-raw-{args.table}')
    for key_name, value in settings.items():
        builder = builder.config(key_name, value)

    # The local Spark driver reads the downloaded file within its container.
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / FILENAMES[args.table]
        s3.download_file('landing', key, str(path))
        spark = builder.getOrCreate()
        try:
            spark.sparkContext.setLogLevel('WARN')
            schema = T.StructType([
                T.StructField(column, T.StringType(), True)
                for column in HEADERS[args.table]
            ])
            frame = (
                spark.read.option('header', True)
                .option('enforceSchema', False)
                .option('mode', 'FAILFAST')
                .option('escape', '"')
                .schema(schema).csv(str(path))
            )
            if is_transaction:
                frame = frame.withColumn('_batch_id', F.lit(args.batch_id))

            spark.sql(
                "CREATE NAMESPACE IF NOT EXISTS lakehouse.`01_raw` "
                "LOCATION 's3://warehouse/01_raw/'"
            )

            table = f"lakehouse.`01_raw`.{args.table}"
            if spark.catalog.tableExists(table):
                predicate = (F.col('_batch_id') == F.lit(args.batch_id)
                             if is_transaction else F.lit(True))
                frame.writeTo(table).overwrite(predicate)
            else:
                writer = (frame.writeTo(table).using('iceberg')
                          .tableProperty('format-version', '2'))
                if is_transaction:
                    writer = writer.partitionedBy('_batch_id')
                writer.create()

            result = spark.table(table)
            if is_transaction:
                result = result.filter(F.col('_batch_id') == F.lit(args.batch_id))
            print(
                f'RAW_LOAD_SUCCESS table={table} batch={args.batch_id} '
                f'rows={result.count()}', flush=True,
            )
        finally:
            spark.stop()


if __name__ == '__main__':
    main()
