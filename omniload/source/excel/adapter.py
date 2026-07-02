# https://github.com/dlt-hub/dlt-studio/blob/devel/dlt/sources/_core_source_templates/filesystem_pipeline.py
from typing import Any, Iterable, Optional

import dlt
import polars as pl
from dlt.extract import DltResource
from dlt.extract import Incremental as dlt_incremental


@dlt.source(name="excel", max_table_nesting=0)
def excel_source(
    uri: str,
    table: str,
    **kwargs: Any,
) -> Iterable[DltResource]:
    """
    Read from an Excel spreadsheet.

    Args:
        uri (str): A filesystem path to the Excel file.
        table (str): The name of the sheet to read from the spreadsheet.

    Returns:
        Iterable[DltResource]: Resources with data.
    """

    incremental_key = kwargs.get("incremental_key")
    merge_key = kwargs.get("merge_key")

    def reader(
        incremental: Optional[dlt_incremental[Any]] = None,
    ):
        df = pl.read_excel(uri, sheet_name=table)

        if incremental_key and incremental_key not in df.columns:
            raise ValueError(
                f"incremental_key '{incremental_key}' not found in sheet '{table}'"
            )

        rows = df.rows(named=True)

        if incremental_key and incremental and incremental.start_value:
            rows = [
                row
                for row in rows
                if row.get(incremental_key) is not None
                and row[incremental_key] >= incremental.start_value
            ]

        return rows

    write_disposition = "merge" if merge_key else "replace"

    return dlt.resource(
        reader,
        name=table,
        write_disposition=write_disposition,
        merge_key=merge_key,
    )(
        incremental=dlt_incremental(
            incremental_key or "",
            initial_value=kwargs.get("interval_start"),
            end_value=kwargs.get("interval_end"),
            range_end="closed",
            range_start="closed",
        )
    )
