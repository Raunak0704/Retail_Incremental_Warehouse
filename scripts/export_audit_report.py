from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from retail_etl.snowflake_ops import connect


QUERIES = {
    "Batch_Log": """
        SELECT BATCH_ID, SOURCE_TABLE, LOAD_MODE, LOWER_WATERMARK, UPPER_WATERMARK,
               UPPER_PRIMARY_KEY, ROWS_EXTRACTED, S3_PREFIX, STATUS, STARTED_AT, COMPLETED_AT
        FROM RETAIL_DW.AUDIT.ETL_BATCH_LOG
        WHERE STARTED_AT >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        ORDER BY STARTED_AT DESC
    """,
    "Watermarks": """
        SELECT SOURCE_TABLE, LAST_SUCCESS_UPDATED_AT, LAST_SUCCESS_PRIMARY_KEY,
               LAST_SUCCESS_BATCH_ID, UPDATED_AT
        FROM RETAIL_DW.CONTROL.ETL_WATERMARK
        ORDER BY SOURCE_TABLE
    """,
    "Data_Quality": """
        SELECT CHECKED_AT, BATCH_ID, SOURCE_TABLE, CHECK_NAME, FAILED_ROWS, STATUS, DETAILS
        FROM RETAIL_DW.AUDIT.DATA_QUALITY_RESULTS
        WHERE CHECKED_AT >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
        ORDER BY CHECKED_AT DESC
    """,
}


def query_dataframe(cur, query: str) -> pd.DataFrame:
    cur.execute(query)
    columns = [item[0] for item in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=columns)


def main():
    parser = argparse.ArgumentParser(description="Export a bounded Snowflake audit reconciliation workbook")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    if args.days < 1 or args.days > 365:
        parser.error("--days must be between 1 and 365")
    output = Path(args.output or f"retail_pipeline_audit_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.xlsx")

    conn = connect()
    try:
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            cur = conn.cursor()
            for sheet, query in QUERIES.items():
                frame = query_dataframe(cur, query.format(days=args.days))
                frame.to_excel(writer, sheet_name=sheet, index=False)
                worksheet = writer.sheets[sheet]
                worksheet.freeze_panes = "A2"
                worksheet.auto_filter.ref = worksheet.dimensions
                for column_cells in worksheet.columns:
                    max_len = min(max(len(str(cell.value or "")) for cell in column_cells) + 2, 60)
                    worksheet.column_dimensions[column_cells[0].column_letter].width = max(12, max_len)
            cur.close()
    finally:
        conn.close()
    print(f"Created {output.resolve()}")


if __name__ == "__main__":
    main()
