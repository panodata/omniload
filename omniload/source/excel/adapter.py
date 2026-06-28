# https://github.com/dlt-hub/dlt-studio/blob/devel/dlt/sources/_core_source_templates/filesystem_pipeline.py
from typing import Iterable

import dlt
import polars as pl
from dlt.extract import DltResource


@dlt.source(name="excel", max_table_nesting=0)
def excel_source(
    uri: str,
    table: str,
) -> Iterable[DltResource]:
    """
    Read from Excel spreadsheet.

    Args:
        uri (str): A filesystem URI that addresses the Delta Lake catalog.
        table (str): <schema>.<table> that addresses the Delta Lake table.

    Returns:
        Iterable[DltResource]: Resources with data.
    """

    def reader():
        return pl.read_excel(uri, sheet_name=table).rows()

    return dlt.resource(
        reader,
        name=table,
        # TODO: Are other write dispositions possible?
        write_disposition="replace",
    )()
