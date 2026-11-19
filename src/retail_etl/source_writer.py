from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


def write_source_rows(
    conn,
    *,
    database: str,
    schema: str,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    chunk_size: int = 5000,
) -> int:
    """Bulk-load generated rows into an existing Snowflake source table."""
    total = 0
    for start in range(0, len(rows), chunk_size):
        batch = rows[start : start + chunk_size]
        frame = pd.DataFrame.from_records(batch, columns=list(columns))
        success, _chunks, rows_loaded, output = write_pandas(
            conn,
            frame,
            table_name=table.upper(),
            database=database.upper(),
            schema=schema.upper(),
            quote_identifiers=False,
            auto_create_table=False,
            overwrite=False,
            chunk_size=chunk_size,
            compression="snappy",
        )
        if not success or rows_loaded != len(frame):
            raise RuntimeError(
                f"Snowflake bulk load failed for {database}.{schema}.{table}: "
                f"expected={len(frame)}, loaded={rows_loaded}, details={output}"
            )
        total += int(rows_loaded)
    return total
