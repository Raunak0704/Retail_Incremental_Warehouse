from __future__ import annotations

from retail_etl.extractor import extract_table_to_s3
from retail_etl.snowflake_ops import (
    commit_watermarks,
    get_watermark,
    load_batch_to_raw,
    run_quality_checks,
    transform_table,
)


def extract_table(table: str) -> dict:
    previous = get_watermark(table)
    return extract_table_to_s3(table, previous)


def load_raw(table: str, manifest: dict) -> dict:
    return load_batch_to_raw(table, manifest)


def transform(table: str, manifest: dict) -> dict:
    return transform_table(table, manifest)


def quality_and_commit(manifests: list[dict]) -> dict:
    quality = run_quality_checks(manifests)
    commit = commit_watermarks(manifests)
    return {"quality": quality, "watermarks": commit}
