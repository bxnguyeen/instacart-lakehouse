from datetime import timedelta
import json

import pendulum

from airflow.sdk import dag, task, Param, get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
import re
from catalog_state import read_catalog_state, write_catalog_state

class StringCommandDockerOperator(DockerOperator):
    def execute(self, context):
        if isinstance(self.command, list):
            self.command = [
                json.dumps(value)
                if isinstance(value, (dict, list, bool))
                else str(value)
                for value in self.command
            ]

        return super().execute(context)

@dag(
    dag_id="instacart_silver",
    start_date=pendulum.datetime(
        2026, 10, 1, tz="Asia/Ho_Chi_Minh"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "batch_id": Param(
            "002",
            type="string",
            enum=["001", "002", "003"],
        ),
        "catalog_version": Param(
            "",
            type="string",
            description="Catalog version from the Bronze run",
        ),
        "refresh_catalog": Param(False, type="boolean"),
        "run_gold": Param(False, type="boolean"),
    },
    render_template_as_native_obj=True,
    tags=["instacart", "incremental", "silver"],
)
def instacart_silver():
    @task
    def resolve_batch():
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        batch_id = conf.get(
            "batch_id",
            context["params"]["batch_id"],
        )

        if batch_id not in {"001", "002", "003"}:
            raise ValueError(f"Unsupported batch_id: {batch_id}")

        return batch_id

    @task
    def resolve_catalog():
        context = get_current_context()
        conf = context["dag_run"].conf or {}

        version = conf.get(
            "catalog_version",
            context["params"]["catalog_version"],
        )
        refresh = conf.get(
            "refresh_catalog",
            context["params"]["refresh_catalog"],
        )

        if (
            not isinstance(version, str)
            or re.fullmatch(r"[0-9a-f]{64}", version) is None
        ):
            raise ValueError(
                "Provide a valid catalog_version from Bronze"
            )

        if not isinstance(refresh, bool):
            raise ValueError("refresh_catalog must be a boolean")

        return {
            "catalog_version": version,
            "refresh_catalog": refresh,
        }

    @task
    def mark_silver_catalog_building(catalog):
        context = get_current_context()

        return write_catalog_state(
            layer="silver",
            catalog_version=catalog["catalog_version"],
            status="building",
            run_id=context["run_id"],
        )

    @task
    def mark_silver_catalog_completed(catalog):
        context = get_current_context()

        return write_catalog_state(
            layer="silver",
            catalog_version=catalog["catalog_version"],
            status="completed",
            run_id=context["run_id"],
        )

    @task
    def inspect_silver_catalog_state(catalog):
        state = read_catalog_state("silver")

        completed_for_version = (
            isinstance(state, dict)
            and state.get("status") == "completed"
            and state.get("catalog_version") == catalog["catalog_version"]
        )

        skip_candidate = (
            completed_for_version
            and not catalog["refresh_catalog"]
        )

        print(
            "SILVER_CATALOG_STATE "
            f"version={catalog['catalog_version']} "
            f"completed_for_version={completed_for_version} "
            f"refresh_catalog={catalog['refresh_catalog']} "
            f"skip_candidate={skip_candidate}",
            flush=True,
        )

        return skip_candidate

    @task.branch
    def choose_silver_catalog_path(skip_candidate):
        if skip_candidate:
            return "validate_existing_silver_catalog"

        return "mark_silver_catalog_building"

    selected_batch = resolve_batch()
    selected_catalog = resolve_catalog()
    catalog_state_checked = inspect_silver_catalog_state(selected_catalog)
    catalog_path = choose_silver_catalog_path(catalog_state_checked)

    batch_template = (
        "{{ ti.xcom_pull(task_ids='resolve_batch') }}"
    )

    common = {
        "image": "instacart-dbt:local",
        "api_version": "auto",
        "docker_url": "unix://var/run/docker.sock",
        "network_mode": "instacart-lakehouse_default",
        "force_pull": False,
        "mount_tmp_dir": False,
        "mem_limit": "1g",
        "auto_remove": "force",
        "do_xcom_push": False,
        "retries": 0,
        "execution_timeout": timedelta(minutes=60),
    }

    catalog_vars = json.dumps({
        "catalog_version": (
            "{{ ti.xcom_pull(task_ids='resolve_catalog')"
            "['catalog_version'] }}"
        ),
    })

    build_catalog = StringCommandDockerOperator(
        task_id="build_silver_catalog",
        command=[
            "build",
            "--project-dir", "/opt/dbt",
            "--profiles-dir", "/opt/dbt",
            "--select",
            "stg_aisles",
            "stg_departments",
            "stg_products",
            "--indirect-selection", "cautious",
            "--threads", "1",
            "--vars", catalog_vars,
        ],
        **common,
    )

    catalog_building = mark_silver_catalog_building(selected_catalog)
    catalog_completed = mark_silver_catalog_completed(selected_catalog)

    validate_existing_catalog = StringCommandDockerOperator(
        task_id="validate_existing_silver_catalog",
        command=[
            "run-operation",
            "validate_silver_catalog",
            "--project-dir", "/opt/dbt",
            "--profiles-dir", "/opt/dbt",
            "--vars", catalog_vars,
        ],
        **common,
    )

    catalog_ready = EmptyOperator(
        task_id="silver_catalog_ready",
        trigger_rule="none_failed_min_one_success",
    )

    selected_batch >> catalog_path

    catalog_path >> catalog_building
    catalog_building >> build_catalog >> catalog_completed
    catalog_completed >> catalog_ready

    catalog_path >> validate_existing_catalog
    validate_existing_catalog >> catalog_ready

    def dbt_task(task_id, action, model, bucket=0):
        selections = [model] if isinstance(model, str) else model
        return StringCommandDockerOperator(
            task_id=task_id,
            command=[
                action,
                "--project-dir", "/opt/dbt",
                "--profiles-dir", "/opt/dbt",
                "--select", *selections,
                "--indirect-selection", "cautious",
                "--threads", "1",
                "--vars",
                json.dumps({
                    "batch_id": batch_template,
                    "merge_bucket": bucket,
                }),
            ],
            **common,
        )

    orders = dbt_task(
        "build_silver_orders_and_train",
        "build",
        [
            "stg_orders_incremental",
            "stg_order_products_train_incremental",
        ],
    )

    catalog_ready >> orders
    previous = orders

    for bucket in range(8):
        merge_prior = dbt_task(
            f"merge_prior_bucket_{bucket:02d}",
            "run",
            "stg_order_products_prior_incremental",
            bucket=bucket,
        )

        previous >> merge_prior
        previous = merge_prior

    final_tests = dbt_task(
        "test_silver_final",
        "test",
        [
            "stg_order_products_prior_incremental",
            "order_items_sources_overlap",
        ],
    )

    previous >> final_tests

    @task.short_circuit
    def should_run_gold():
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        value = conf.get(
            "run_gold",
            context["params"]["run_gold"],
        )

        if not isinstance(value, bool):
            raise ValueError("run_gold must be a boolean")

        return value

    gold_gate = should_run_gold()

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="instacart_gold",
        trigger_run_id="silver__{{ run_id }}",
        logical_date=None,
        conf={
            "batch_id": batch_template,
            "catalog_version": (
                "{{ ti.xcom_pull(task_ids='resolve_catalog')"
                "['catalog_version'] }}"
            ),
            "refresh_catalog": (
                "{{ ti.xcom_pull(task_ids='resolve_catalog')"
                "['refresh_catalog'] }}"
            ),
        },
        wait_for_completion=False,
        skip_when_already_exists=True,
    )

    final_tests >> gold_gate >> trigger_gold


instacart_silver()