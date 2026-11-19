from __future__ import annotations

import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector

from retail_etl.config import TABLES, pipeline_config, source_snowflake_config
from retail_etl.hwm import lookback_lower_bound, serializable_watermark
from retail_etl.s3_landing import upload_batch

MAX_BIGINT = 9_223_372_036_854_775_807


def _get_source_upper(conn) -> tuple[datetime, int]:
    """Capture a fixed Snowflake source-clock cutoff for this extraction batch."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT CURRENT_TIMESTAMP()")
        row = cur.fetchone()
        return row[0], MAX_BIGINT
    finally:
        cur.close()


def _write_parquet_chunks(
    conn,
    *,
    database: str,
    schema: str,
    table: str,
    pk: str,
    mode: str,
    upper: tuple[datetime, int] | None,
    lower_ts: datetime | None,
    chunk_size: int,
    output_dir: Path,
    compression: str,
) -> tuple[list[Path], int]:
    """Fetch a Snowflake result set in bounded batches and write Parquet parts."""
    if upper is None:
        return [], 0

    qualified_table = f'"{database}"."{schema}"."{table}"'
    quoted_pk = f'"{pk}"'
    upper_ts, upper_pk = upper

    if mode == "full":
        query = (
            f"SELECT * FROM {qualified_table} "
            f"WHERE (UPDATED_AT < %s OR (UPDATED_AT = %s AND {quoted_pk} <= %s))"
        )
        params = (upper_ts, upper_ts, upper_pk)
    else:
        # Inclusive lower bound deliberately replays the configured lookback window.
        # Snowflake target staging and MERGEs deduplicate the overlap idempotently.
        query = (
            f"SELECT * FROM {qualified_table} "
            f"WHERE (UPDATED_AT > %s OR (UPDATED_AT = %s AND {quoted_pk} >= %s)) "
            f"AND (UPDATED_AT < %s OR (UPDATED_AT = %s AND {quoted_pk} <= %s))"
        )
        params = (lower_ts, lower_ts, 0, upper_ts, upper_ts, upper_pk)

    cur = conn.cursor()
    files: list[Path] = []
    total = 0
    try:
        cur.execute(query, params)
        columns = [description[0] for description in cur.description]
        part_number = 0
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                break
            records = [
                {name.upper(): value for name, value in zip(columns, row)}
                for row in rows
            ]
            # Store timestamps as ISO-8601 strings so Snowflake VARIANT casts are explicit.
            for record in records:
                for column, value in record.items():
                    if isinstance(value, datetime):
                        record[column] = value.isoformat()
            table_arrow = pa.Table.from_pylist(records)
            file_path = output_dir / f"part-{part_number:05d}.parquet"
            pq.write_table(table_arrow, file_path, compression=compression)
            files.append(file_path)
            total += len(records)
            part_number += 1
    finally:
        cur.close()

    return files, total


def extract_table_to_s3(table: str, previous_watermark: dict | None) -> dict:
    """Extract one Snowflake source table in bounded chunks and upload immutable Parquet files."""
    if table not in TABLES:
        raise ValueError(f"Unsupported source table: {table}")

    cfg = pipeline_config()
    table_cfg = TABLES[table]
    batch_id = uuid.uuid4().hex
    batch_started_at = datetime.now(timezone.utc)
    previous = previous_watermark

    conn = snowflake.connector.connect(**source_snowflake_config())
    try:
        upper = _get_source_upper(conn)
        if previous is None or not previous.get("updated_at"):
            mode = "full"
            lower_ts = None
            lower_watermark = None
        else:
            mode = "incremental"
            last_ts = previous["updated_at"]
            if isinstance(last_ts, str):
                last_ts = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
            lower_ts = lookback_lower_bound(last_ts, cfg["lookback_minutes"])
            lower_watermark = serializable_watermark(previous)

        with tempfile.TemporaryDirectory(prefix=f"retail_{table}_") as temp_dir:
            files, row_count = _write_parquet_chunks(
                conn,
                database=cfg["source_database"].upper(),
                schema=table_cfg.source_schema.upper(),
                table=table_cfg.source_table.upper(),
                pk=table_cfg.primary_key.upper(),
                mode=mode,
                upper=upper,
                lower_ts=lower_ts,
                chunk_size=cfg["chunk_size"],
                output_dir=Path(temp_dir),
                compression=cfg["parquet_compression"],
            )
            upper_dict = {
                "updated_at": upper[0].isoformat(),
                "primary_key": int(upper[1]),
            }
            manifest = {
                "project": "retail_incremental_warehouse",
                "source_platform": "snowflake",
                "source_database": cfg["source_database"],
                "source_schema": table_cfg.source_schema,
                "source_table": table,
                "batch_id": batch_id,
                "load_mode": mode,
                "lower_watermark": lower_watermark,
                "lower_bound_inclusive": lower_ts.isoformat() if lower_ts else None,
                "upper_watermark": upper_dict,
                "rows_extracted": row_count,
                "files": [p.name for p in files],
                "batch_started_at_utc": batch_started_at.isoformat(),
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            s3_prefix = upload_batch(
                bucket=cfg["s3_bucket"],
                base_prefix=cfg["s3_prefix"],
                table=table,
                batch_id=batch_id,
                local_files=files,
                manifest=manifest,
                region=cfg["aws_region"],
                sse=cfg["s3_sse"],
            )

        return {
            **manifest,
            "s3_bucket": cfg["s3_bucket"],
            "s3_prefix": s3_prefix,
            "upper_updated_at": upper[0].isoformat(),
            "upper_primary_key": int(upper[1]),
            "extract_status": "SUCCESS",
        }
    finally:
        conn.close()
