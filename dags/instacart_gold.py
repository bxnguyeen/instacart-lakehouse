from datetime import timedelta
import json

import pendulum

from airflow.sdk import dag, task, Param, get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
import re
from catalog_state import read_catalog_state, write_catalog_state
from airflow.providers.standard.operators.empty import EmptyOperator


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
    dag_id="instacart_gold",
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
            description="Catalog version from the Silver run",
        ),
        "refresh_catalog": Param(False, type="boolean"),
    },
    tags=["instacart", "incremental", "gold"],
)
def instacart_gold():
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
                "Provide a valid catalog_version from Silver"
            )

        if not isinstance(refresh, bool):
            raise ValueError("refresh_catalog must be a boolean")

        return {
            "catalog_version": version,
            "refresh_catalog": refresh,
        }

    @task
    def mark_gold_catalog_building(catalog):
        context = get_current_context()

        return write_catalog_state(
            layer="gold",
            catalog_version=catalog["catalog_version"],
            status="building",
            run_id=context["run_id"],
        )

    @task
    def mark_gold_catalog_completed(catalog):
        context = get_current_context()

        return write_catalog_state(
            layer="gold",
            catalog_version=catalog["catalog_version"],
            status="completed",
            run_id=context["run_id"],
        )

    @task.branch
    def choose_gold_catalog_path(catalog):
        state = read_catalog_state("gold")
        skip_candidate = (
            not catalog["refresh_catalog"]
            and isinstance(state, dict)
            and state.get("status") == "completed"
            and state.get("catalog_version") == catalog["catalog_version"]
        )
        print(
            f"GOLD_CATALOG_STATE version={catalog['catalog_version']} "
            f"skip_candidate={skip_candidate}",
            flush=True,
        )
        if skip_candidate:
            return "validate_existing_gold_catalog"
        return "validate_silver_catalog_for_gold"

    selected_batch = resolve_batch()
    selected_catalog = resolve_catalog()
    catalog_path = choose_gold_catalog_path(selected_catalog)
    selected_batch >> catalog_path

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

    def catalog_validation(task_id, include_gold):
        return StringCommandDockerOperator(
            task_id=task_id,
            command=[
                "run-operation", "validate_gold_catalog",
                "--project-dir", "/opt/dbt",
                "--profiles-dir", "/opt/dbt",
                "--vars", catalog_vars,
                "--args", json.dumps({"include_gold": include_gold}),
            ],
            **common,
        )

    validate_silver = catalog_validation(
        "validate_silver_catalog_for_gold", False
    )
    validate_existing = catalog_validation(
        "validate_existing_gold_catalog", True
    )
    validate_built = catalog_validation(
        "validate_built_gold_catalog", True
    )
    catalog_ready = EmptyOperator(
        task_id="gold_catalog_ready",
        trigger_rule="none_failed_min_one_success",
    )

    def dbt_task(task_id, action, selections):
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
                }),
            ],
            **common,
        )

    build_products = dbt_task(
        task_id="build_and_test_dim_products",
        action="build",
        selections=[
            "dim_products",
            "dim_products_row_count",
        ],
    )

    catalog_building = mark_gold_catalog_building(selected_catalog)
    catalog_completed = mark_gold_catalog_completed(selected_catalog)

    catalog_path >> validate_silver >> catalog_building
    catalog_building >> build_products >> validate_built
    validate_built >> catalog_completed >> catalog_ready

    catalog_path >> validate_existing >> catalog_ready

    build_gold = dbt_task(
        task_id="build_gold_transactions",
        action="build",
        selections=[
            "fct_order_items_incremental",
            "fct_orders_incremental",
            "fct_order_items_row_count",
            "fct_orders_row_count",
            "fct_orders_items_total",
            "fct_orders_metrics",
        ],
    )
    catalog_ready >> build_gold


instacart_gold()