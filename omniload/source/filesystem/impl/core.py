from typing import Any, Iterator, Optional, Union

import dlt
from dlt.common.storages.configuration import FileSystemCredentials
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from fsspec import AbstractFileSystem

from omniload.source.filesystem.adapter import filesystem, readers
from omniload.source.filesystem.format.readers import _read_csv_headless


def resource_for_reader(
    bucket_url: str,
    credentials: Union[FileSystemCredentials, AbstractFileSystem],
    file_glob: str,
    reader_name: str,
    column_types: Optional[dict[str, Any]] = None,
) -> Any:
    if reader_name != "read_csv_headless":
        return readers(bucket_url, credentials, file_glob).with_resources(reader_name)

    column_names = list(column_types.keys()) if column_types else None

    def read_csv_headless_with_cols(
        items: Iterator[FileItemDict],
        chunksize: int = 10000,
        **pandas_kwargs: Any,
    ) -> Iterator[TDataItems]:
        yield from _read_csv_headless(
            items,
            chunksize=chunksize,
            column_names=column_names,
            **pandas_kwargs,
        )

    filesystem_resource = filesystem(bucket_url, credentials, file_glob=file_glob)
    return filesystem_resource | dlt.transformer(
        name="read_csv_headless", max_table_nesting=0
    )(read_csv_headless_with_cols)
