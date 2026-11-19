from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import snowflake.connector

from retail_etl.config import pipeline_config, snowflake_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = PROJECT_ROOT / "sql"

TRANSFORM_SQL = {
    "customers": "10_customer_scd2.sql",
    "products": "11_product_scd1.sql",
    "orders": "12_orders_incremental.sql",
    "order_items": "13_order_items_incremental.sql",
}


def connect():
    return snowflake.connector.connect(**snowflake_config())


def _safe_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value.upper()


def _statements(script: str) -> list[str]:
    # Project SQL files intentionally avoid semicolons inside literals/comments.
    return [part.strip() for part in script.split(";") if part.strip()]


def _run_script(cursor, filename: str, batch_id: str):
    script = (SQL_DIR / filename).read_text(encoding="utf-8")
    script = script.replace("{{BATCH_ID}}", batch_id)
    for statement in _statements(script):
        cursor.execute(statement)


def get_watermark(source_table: str) -> dict[str, Any] | None:
    table = _safe_identifier(source_table)
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT LAST_SUCCESS_UPDATED_AT, LAST_SUCCESS_PRIMARY_KEY "
            "FROM RETAIL_DW.CONTROL.ETL_WATERMARK WHERE SOURCE_TABLE = %s",
            (table,),
        )
        row = cur.fetchone()
        cur.close()
        if not row or row[0] is None:
            return None
        return {"updated_at": row[0], "primary_key": int(row[1] or 0)}
    finally:
        conn.close()


def load_batch_to_raw(source_table: str, manifest: dict) -> dict:
    table = _safe_identifier(source_table)
    if int(manifest.get("rows_extracted", 0)) == 0:
        return {"source_table": table, "batch_id": manifest["batch_id"], "loaded": 0}

    cfg = pipeline_config()
    db = _safe_identifier(cfg["database"])
    stage = _safe_identifier(cfg["stage"])
    target = f"{db}.RAW.RAW_{table}"
    expected_root = cfg["s3_prefix"].rstrip("/") + "/"
    full_prefix = manifest["s3_prefix"]
    if not full_prefix.startswith(expected_root):
        raise ValueError("Manifest S3 prefix is outside the configured project prefix")
    stage_relative_path = full_prefix[len(expected_root):]

    conn = connect()
    try:
        cur = conn.cursor()
        copy_sql = f"""
            COPY INTO {target} (RECORD, BATCH_ID, SOURCE_FILE, LOADED_AT)
            FROM (
              SELECT $1, '{manifest['batch_id']}', METADATA$FILENAME, CURRENT_TIMESTAMP()
              FROM @{db}.RAW.{stage}/{stage_relative_path}
            )
            FILE_FORMAT = (FORMAT_NAME = {db}.RAW.RETAIL_PARQUET_FF)
            PATTERN = '.*[.]parquet'
            ON_ERROR = 'ABORT_STATEMENT'
        """
        cur.execute(copy_sql)
        results = cur.fetchall()
        cur.close()
        conn.commit()
        # Snowflake COPY result row: file, status, rows_parsed, rows_loaded, error.
        rows_loaded = sum(int(row[3] or 0) for row in results if len(row) > 3)
        return {
            "source_table": table,
            "batch_id": manifest["batch_id"],
            "loaded": rows_loaded,
            "copy_files": len(results),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def transform_table(source_table: str, manifest: dict) -> dict:
    table = _safe_identifier(source_table)
    if table.lower() not in TRANSFORM_SQL or int(manifest.get("rows_extracted", 0)) == 0:
        return {"source_table": table, "batch_id": manifest["batch_id"], "transformed": 0}

    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        _run_script(cur, TRANSFORM_SQL[table.lower()], manifest["batch_id"])
        conn.commit()
        cur.close()
        return {
            "source_table": table,
            "batch_id": manifest["batch_id"],
            "transformed": int(manifest.get("rows_extracted", 0)),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _record_quality(cur, batch_id: str, source_table: str, check_name: str, failed_rows: int, details: str):
    status = "PASS" if failed_rows == 0 else "FAIL"
    cur.execute(
        "INSERT INTO RETAIL_DW.AUDIT.DATA_QUALITY_RESULTS "
        "(BATCH_ID, SOURCE_TABLE, CHECK_NAME, FAILED_ROWS, STATUS, DETAILS) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (batch_id, source_table, check_name, failed_rows, status, details[:3900]),
    )
    return failed_rows > 0


def _scalar(cur, query: str, params: tuple = ()) -> int:
    cur.execute(query, params)
    value = cur.fetchone()[0]
    return int(value or 0)


def run_quality_checks(manifests: list[dict]) -> dict:
    conn = connect()
    all_checks = 0
    failures: list[str] = []
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        # Global uniqueness/integrity checks plus batch-specific relationship checks.
        checks = [
            ("CUSTOMERS", "multiple_current_customer_versions",
             "SELECT COUNT(*) FROM (SELECT CUSTOMER_ID FROM RETAIL_DW.DW.DIM_CUSTOMER "
             "WHERE IS_CURRENT = TRUE GROUP BY CUSTOMER_ID HAVING COUNT(*) > 1)"),
            ("CUSTOMERS", "invalid_customer_effective_intervals",
             "SELECT COUNT(*) FROM RETAIL_DW.DW.DIM_CUSTOMER WHERE VALID_TO <= VALID_FROM"),
            ("CUSTOMERS", "overlapping_customer_effective_intervals",
             "SELECT COUNT(*) FROM (SELECT CUSTOMER_ID, VALID_FROM, "
             "LAG(VALID_TO) OVER (PARTITION BY CUSTOMER_ID ORDER BY VALID_FROM) AS PREVIOUS_VALID_TO "
             "FROM RETAIL_DW.DW.DIM_CUSTOMER) WHERE PREVIOUS_VALID_TO > VALID_FROM"),
            ("PRODUCTS", "duplicate_product_business_keys",
             "SELECT COUNT(*) FROM (SELECT PRODUCT_ID FROM RETAIL_DW.DW.DIM_PRODUCT "
             "GROUP BY PRODUCT_ID HAVING COUNT(*) > 1)"),
            ("ORDERS", "duplicate_order_business_keys",
             "SELECT COUNT(*) FROM (SELECT ORDER_ID FROM RETAIL_DW.DW.FACT_ORDERS "
             "GROUP BY ORDER_ID HAVING COUNT(*) > 1)"),
            ("ORDER_ITEMS", "duplicate_order_item_business_keys",
             "SELECT COUNT(*) FROM (SELECT ORDER_ITEM_ID FROM RETAIL_DW.DW.FACT_ORDER_ITEMS "
             "GROUP BY ORDER_ITEM_ID HAVING COUNT(*) > 1)"),
        ]
        for source_table, name, query in checks:
            failed = _scalar(cur, query)
            if _record_quality(cur, "BATCH_GROUP", source_table, name, failed, "Warehouse-wide uniqueness check"):
                failures.append(f"{source_table}.{name}: {failed}")
            all_checks += 1

        raw_stage_tables = {
            "CUSTOMERS": ("RAW_CUSTOMERS", "STG_CUSTOMERS"),
            "PRODUCTS": ("RAW_PRODUCTS", "STG_PRODUCTS"),
            "ORDERS": ("RAW_ORDERS", "STG_ORDERS"),
            "ORDER_ITEMS": ("RAW_ORDER_ITEMS", "STG_ORDER_ITEMS"),
        }
        for manifest in manifests:
            table = manifest["source_table"].upper()
            batch_id = manifest["batch_id"]
            raw_table, stage_table = raw_stage_tables[table]
            raw_count = _scalar(cur, f"SELECT COUNT(*) FROM RETAIL_DW.RAW.{raw_table} WHERE BATCH_ID = %s", (batch_id,))
            stage_count = _scalar(cur, f"SELECT COUNT(*) FROM RETAIL_DW.STAGING.{stage_table} WHERE BATCH_ID = %s", (batch_id,))
            expected_count = int(manifest.get("rows_extracted", 0))
            count_diff = abs(expected_count - raw_count) + abs(expected_count - stage_count)
            if _record_quality(cur, batch_id, table, "extract_raw_stage_row_count", count_diff,
                               f"Expected={expected_count}; raw={raw_count}; staging={stage_count}"):
                failures.append(f"{table}.extract_raw_stage_row_count: expected={expected_count}, raw={raw_count}, staging={stage_count}")
            all_checks += 1

            if table == "ORDERS" and manifest.get("rows_extracted", 0):
                failed = _scalar(cur,
                    "SELECT COUNT(*) FROM RETAIL_DW.STAGING.STG_ORDERS S "
                    "JOIN RETAIL_DW.DW.FACT_ORDERS F ON F.ORDER_ID = S.ORDER_ID "
                    "WHERE S.BATCH_ID = %s AND F.CUSTOMER_SK IS NULL", (batch_id,))
                if _record_quality(cur, batch_id, table, "orders_resolve_customer_dimension", failed,
                                   "Every staged order should resolve to an SCD2 customer version at order time"):
                    failures.append(f"{table}.orders_resolve_customer_dimension: {failed}")
                all_checks += 1
            elif table == "ORDER_ITEMS" and manifest.get("rows_extracted", 0):
                failed = _scalar(cur,
                    "SELECT COUNT(*) FROM RETAIL_DW.STAGING.STG_ORDER_ITEMS S "
                    "JOIN RETAIL_DW.DW.FACT_ORDER_ITEMS F ON F.ORDER_ITEM_ID = S.ORDER_ITEM_ID "
                    "WHERE S.BATCH_ID = %s AND (F.PRODUCT_SK IS NULL OR F.CUSTOMER_SK IS NULL)",
                    (batch_id,))
                if _record_quality(cur, batch_id, table, "order_items_resolve_dimensions", failed,
                                   "Every order line should resolve its product and customer surrogate keys"):
                    failures.append(f"{table}.order_items_resolve_dimensions: {failed}")
                all_checks += 1
        conn.commit()
        cur.close()
        if failures:
            raise RuntimeError("Data-quality gate failed: " + "; ".join(failures))
        return {"status": "PASS", "checks_run": all_checks}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def commit_watermarks(manifests: list[dict]) -> dict:
    """Atomically audit all source batches and advance all non-null watermarks."""
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        for manifest in manifests:
            table = _safe_identifier(manifest["source_table"])
            batch_id = manifest["batch_id"]
            upper_ts = manifest.get("upper_updated_at")
            upper_pk = manifest.get("upper_primary_key")
            lower = manifest.get("lower_bound_inclusive")

            cur.execute(
                "INSERT INTO RETAIL_DW.AUDIT.ETL_BATCH_LOG "
                "(BATCH_ID, SOURCE_TABLE, LOAD_MODE, LOWER_WATERMARK, UPPER_WATERMARK, "
                "UPPER_PRIMARY_KEY, ROWS_EXTRACTED, S3_PREFIX, STATUS, STARTED_AT, COMPLETED_AT) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'SUCCESS', %s::TIMESTAMP_TZ, CURRENT_TIMESTAMP())",
                (batch_id, table, manifest["load_mode"], lower, upper_ts, upper_pk,
                 int(manifest.get("rows_extracted", 0)), manifest.get("s3_prefix", ""),
                 manifest.get("batch_started_at_utc", manifest.get("created_at_utc"))),
            )

            if upper_ts is not None and upper_pk is not None:
                cur.execute(
                    "MERGE INTO RETAIL_DW.CONTROL.ETL_WATERMARK T "
                    "USING (SELECT %s AS SOURCE_TABLE, %s::TIMESTAMP_TZ AS UPDATED_AT, "
                    "%s::NUMBER(38,0) AS PRIMARY_KEY, %s AS BATCH_ID) S "
                    "ON T.SOURCE_TABLE = S.SOURCE_TABLE "
                    "WHEN MATCHED THEN UPDATE SET "
                    "T.LAST_SUCCESS_UPDATED_AT = GREATEST(COALESCE(T.LAST_SUCCESS_UPDATED_AT, S.UPDATED_AT), S.UPDATED_AT), "
                    "T.LAST_SUCCESS_PRIMARY_KEY = IFF(S.UPDATED_AT > T.LAST_SUCCESS_UPDATED_AT OR "
                    "(S.UPDATED_AT = T.LAST_SUCCESS_UPDATED_AT AND S.PRIMARY_KEY >= T.LAST_SUCCESS_PRIMARY_KEY), "
                    "S.PRIMARY_KEY, T.LAST_SUCCESS_PRIMARY_KEY), "
                    "T.LAST_SUCCESS_BATCH_ID = S.BATCH_ID, T.UPDATED_AT = CURRENT_TIMESTAMP() "
                    "WHEN NOT MATCHED THEN INSERT "
                    "(SOURCE_TABLE, LAST_SUCCESS_UPDATED_AT, LAST_SUCCESS_PRIMARY_KEY, LAST_SUCCESS_BATCH_ID) "
                    "VALUES (S.SOURCE_TABLE, S.UPDATED_AT, S.PRIMARY_KEY, S.BATCH_ID)",
                    (table, upper_ts, upper_pk, batch_id),
                )
        conn.commit()
        cur.close()
        return {"status": "COMMITTED", "tables": len(manifests)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
