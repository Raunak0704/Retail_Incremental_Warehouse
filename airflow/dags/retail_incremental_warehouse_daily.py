from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from retail_etl.pipeline import (
    extract_table,
    load_raw,
    quality_and_commit,
    transform,
)

TABLES = ("customers", "products", "orders", "order_items")


@dag(
    dag_id="retail_incremental_warehouse_daily",
    description="HWM-driven retail ingestion, SCD1/SCD2 and incremental fact loading",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["retail", "snowflake", "aws-s3", "high-watermark", "scd"],
)
def retail_incremental_warehouse_daily():
    @task
    def extract(source_table: str) -> dict:
        return extract_table(source_table)

    @task
    def copy_to_raw(source_table: str, manifest: dict) -> dict:
        return load_raw(source_table, manifest)

    @task
    def apply_transform(source_table: str, manifest: dict) -> dict:
        return transform(source_table, manifest)

    @task
    def quality_gate_and_commit(manifests: list[dict], transform_results: list[dict]) -> dict:
        # transform_results is an explicit dependency barrier for every transform branch.
        if len(transform_results) != len(manifests):
            raise RuntimeError("Not all source-table transformation branches completed")
        return quality_and_commit(manifests)

    manifests = {}
    transformed = {}
    for table in TABLES:
        manifests[table] = extract.override(task_id=f"extract_{table}")(table)
        raw_loaded = copy_to_raw.override(task_id=f"copy_{table}_to_snowflake_raw")(
            table, manifests[table]
        )
        transformed[table] = apply_transform.override(task_id=f"transform_{table}")(
            table, manifests[table]
        )
        raw_loaded >> transformed[table]

    # Dimensions precede facts so the fact merges can resolve surrogate keys.
    transformed["customers"] >> transformed["orders"]
    transformed["products"] >> transformed["order_items"]
    transformed["orders"] >> transformed["order_items"]

    quality_gate_and_commit(list(manifests.values()), list(transformed.values()))


retail_incremental_warehouse_daily()
