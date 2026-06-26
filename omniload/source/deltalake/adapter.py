from typing import Iterable, Optional

import dlt
import polars as pl
from dlt.extract import DltResource
from yarl import URL


@dlt.source(name="deltalake", max_table_nesting=0)
def deltalake_source(
    uri: str,
    table: str,
    batch_size: Optional[int] = 75_000,
) -> Iterable[DltResource]:
    """
    Read from Delta Lake tables.

    Args:
        uri (str): A filesystem URI that addresses the Delta Lake catalog.
        table (str): <schema>.<table> that addresses the Delta Lake table.
        batch_size (int): Batch size for Polars

    Returns:
        Iterable[DltResource]: Resources with data in RANDOM ORDER (optimized for speed).
    """

    url = URL(uri)
    storage_options = dict(url.query)
    url = url.with_query(None)

    if url.scheme != "uc":
        table_fields = table.split(".")
        if len(table_fields) != 2:
            raise ValueError("Table name must be in the format <schema>.<table>")
        url = url.joinpath(table_fields[-2], table_fields[-1])

    uri = str(url)

    with pl.Config(streaming_chunk_size=batch_size):

        def reader():
            frame = pl.scan_delta(uri, storage_options=storage_options)
            for i, batch in enumerate(
                frame.collect_batches(engine="streaming", chunk_size=batch_size)
            ):
                yield batch

        return dlt.resource(
            reader,
            name=table,
            # TODO: Are other write dispositions possible?
            write_disposition="replace",
        )()
