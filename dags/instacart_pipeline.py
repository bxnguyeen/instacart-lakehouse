"""Run raw ingestion and per-table dbt transformations in one DAG."""
from datetime import timedelta
import json
import logging
import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pendulum
from airflow.sdk import dag, Param, TaskGroup
from airflow.providers.docker.operators.docker import DockerOperator

TABLES = [
    'aisles', 'departments', 'products', 'orders',
    'order_products_prior', 'order_products_train',
]
CATALOG_TABLES = {'aisles', 'departments', 'products'}
BATCH = '{{ params.batch_id }}'


class RawTableOperator(DockerOperator):
    """Upload one CSV, then load its raw table using Spark."""

    def __init__(self, *, table, **kwargs):
        self.table = table
        super().__init__(**kwargs)

    def execute(self, context):
        batch_id = context['params']['batch_id']
        filename = self.table.replace('order_products_', 'order_products__') + '.csv'
        if self.table in CATALOG_TABLES:
            path = Path('/opt/airflow/data/instacart') / filename
            key = f'instacart/catalog/{filename}'
        else:
            path = Path('/opt/airflow/data/batches') / batch_id / filename
            key = f'instacart/batches/{batch_id}/{filename}'

        s3 = boto3.client(
            's3', endpoint_url=os.environ['MINIO_ENDPOINT'],
            aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
            aws_secret_access_key=os.environ['MINIO_SECRET_KEY'],
            region_name='us-east-1',
            config=Config(s3={'addressing_style': 'path'}),
        )
        for bucket in ['landing', 'warehouse']:
            try:
                s3.head_bucket(Bucket=bucket)
            except ClientError as error:
                if error.response['Error']['Code'] not in {'404', 'NoSuchBucket', 'NotFound'}:
                    raise
                try:
                    s3.create_bucket(Bucket=bucket)
                except ClientError as create_error:
                    if create_error.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                        raise

        s3.upload_file(str(path), 'landing', key)
        logging.info('Uploaded %s to s3://landing/%s', filename, key)
        return super().execute(context)


@dag(
    dag_id='instacart_pipeline',
    start_date=pendulum.datetime(2026, 10, 1, tz='Asia/Ho_Chi_Minh'),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={'batch_id': Param('001', type='string', enum=['001', '002', '003'])},
    tags=['instacart', 'raw', 'stg', 'dwh'],
)
def instacart_pipeline():
    common = {
        'api_version': 'auto',
        'docker_url': 'unix://var/run/docker.sock',
        'network_mode': 'instacart-lakehouse_default',
        'force_pull': False,
        'mount_tmp_dir': False,
        'auto_remove': 'force',
        'do_xcom_push': False,
        'retries': 0,
        'execution_timeout': timedelta(minutes=60),
    }

    with TaskGroup('raw') as raw_group:
        for table in TABLES:
            command = [
                '/opt/spark/bin/spark-submit', '--master', 'local[2]',
                '--driver-memory', '1g', '/opt/jobs/load_raw.py',
                '--table', table,
            ]
            if table not in CATALOG_TABLES:
                command += ['--batch-id', BATCH]
            RawTableOperator(
                task_id=f'load_{table}', table=table,
                image='instacart-spark:local', command=command,
                environment={'MINIO_ENDPOINT': 'http://minio:9000', 'AWS_REGION': 'us-east-1'},
                private_environment={
                    'MINIO_ACCESS_KEY': os.environ['MINIO_ACCESS_KEY'],
                    'MINIO_SECRET_KEY': os.environ['MINIO_SECRET_KEY'],
                    'AWS_ACCESS_KEY_ID': os.environ['MINIO_ACCESS_KEY'],
                    'AWS_SECRET_ACCESS_KEY': os.environ['MINIO_SECRET_KEY'],
                },
                mem_limit='2g', **common,
            )

    def dbt_task(task_id, action, selections):
        return DockerOperator(
            task_id=task_id, image='instacart-dbt:local',
            command=[
                action, '--project-dir', '/opt/dbt', '--profiles-dir', '/opt/dbt',
                '--select', *selections,
                '--indirect-selection', 'cautious', '--threads', '1',
                '--vars', json.dumps({'batch_id': BATCH}),
            ],
            mem_limit='1g', **common,
        )

    with TaskGroup('stg') as stg_group:
        staging = {
            table: dbt_task(f'build_{table}', 'build', [f'stg_{table}'])
            for table in TABLES
        }
        staging['aisles'] >> staging['products']
        staging['departments'] >> staging['products']

    stg_checks = dbt_task(
        'test_stg_consistency',
        'test',
        [
            'path:models/stg,test_name:relationships',
            'order_items_sources_overlap',
        ],
    )

    with TaskGroup('dwh') as dwh_group:
        products = dbt_task('build_dim_products', 'build', ['dim_products'])
        items = dbt_task('build_fct_order_items', 'build', ['fct_order_items'])
        orders = dbt_task('build_fct_orders', 'build', ['fct_orders'])
        items >> orders

    dwh_checks = dbt_task(
        'test_dwh_consistency', 
        'test', 
        [
            'dim_products_row_count', 'fct_order_items_row_count',
            'fct_orders_row_count', 'fct_orders_items_total', 'fct_orders_metrics',
        ],
    )
    raw_group >> stg_group >> stg_checks >> dwh_group >> dwh_checks


instacart_pipeline()
