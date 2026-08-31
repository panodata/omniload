from typing import Iterable, Optional

import dlt
import polars as pl
from dlt.extract import DltResource
from yarl import URL

from omniload.core.tablename import two_level
from omniload.error import ValidationError

#: A storage-backed catalog addresses a table as ``<schema>.<table>``, and those two
#: components are the path below the catalog root. Parsing them with the project's
#: shared grammar keeps a quoted component that contains a dot readable here, as it
#: already is on the destination side.
TABLE_CAPABILITY = two_level("Delta Lake")

DEFAULT_BATCH_SIZE = 75_000


def _path_component(value: str, label: str) -> str:
    """Reject an identifier that would address more than one directory.

    The shared grammar validates SQL identifiers, where a quoted component may
    hold any character. Here each component becomes one directory below the
    catalog root, so a separator or a parent reference would silently widen the
    address, `".."` most of all.
    """
    if value in {".", ".."} or "/" in value or "\\" in value:
        raise ValidationError(
            f"The Delta Lake {label} {value!r} is not a single directory name. "
            "Each component addresses one directory below the catalog root, so "
            "it cannot contain a path separator or name a parent directory."
        )
    return value


@dlt.source(name="deltalake", max_table_nesting=0)
def deltalake_source(
    uri: str,
    table: str,
    batch_size: Optional[int] = DEFAULT_BATCH_SIZE,
) -> Iterable[DltResource]:
    """
    Read from Delta Lake tables.

    Args:
        uri (str): A filesystem URI that addresses the Delta Lake catalog.
        table (str): <schema>.<table> that addresses the Delta Lake table.
        batch_size (int): Batch size for Polars

    Returns:
        Iterable[DltResource]: Resources with data in random order,
                               optimized for speed.
    """

    url = URL(uri)
    storage_options = dict(url.query)
    url = url.with_query(None)

    if url.scheme != "uc":
        parsed = TABLE_CAPABILITY.parse(table)
        if parsed.schema is None:
            raise TABLE_CAPABILITY.unresolved_schema_error(table)
        url = url.joinpath(
            _path_component(parsed.schema, "schema"),
            _path_component(parsed.table, "table"),
        )

    uri = str(url)
    chunk_size = batch_size or DEFAULT_BATCH_SIZE

    def reader():
        frame = pl.scan_delta(uri, storage_options=storage_options)
        yield from frame.collect_batches(engine="streaming", chunk_size=chunk_size)

    return dlt.resource(
        reader,
        name=table,
        # Every run reads the whole table, so the resource replaces what it loaded
        # last time. `DeltaLakeSource.honours_run_disposition` lets an explicit
        # `--incremental-strategy append` override this at run level.
        write_disposition="replace",
    )()
