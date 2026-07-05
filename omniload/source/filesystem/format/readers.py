# Copyright 2022-2025 ScaleVector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

from dlt.common import json
from dlt.common.typing import copy_sig
from dlt.sources import DltResource, DltSource, TDataItems
from dlt.sources.filesystem import FileItemDict

from omniload.source.filesystem.format.helpers import fetch_arrow, fetch_json
from omniload.source.filesystem.format.settings import DEFAULT_CHUNK_SIZE


def read_csv(
    items: Iterator[FileItemDict], chunksize: int = 10000, **polars_kwargs: Any
) -> Iterator[TDataItems]:
    """CSV reader using Polars.

    Args:
        chunksize (int): Number of records to read in one chunk
        **polars_kwargs: Additional keyword arguments passed to polars.read_csv
    Returns:
        TDataItem: The file content
    """
    import polars as pl

    # apply defaults to Polars kwargs
    kwargs: Dict[str, Any] = {**{"batch_size": chunksize}, **polars_kwargs}

    for file_obj in items:
        # Read the file in chunks to avoid loading the whole file into memory.
        with file_obj.open() as file:
            df = pl.read_csv(file, **kwargs)
            yield df.to_dicts()


def read_csv_headless(
    items: Iterator[FileItemDict],
    chunksize: int = 10000,
    column_names: Optional[List[str]] = None,
    **polars_kwargs: Any,
) -> Iterator[TDataItems]:
    """CSV reader using Polars. Reads CSV file without headers, using provided column names or generating them.

    Args:
        chunksize (int): Number of records to read in one chunk
        column_names (list[str], optional): Column names for the CSV. If not provided,
            columns will be named unknown_col_0, unknown_col_1, etc.
        **polars_kwargs: Additional keyword arguments passed to polars.read_csv
    Returns:
        TDataItem: The file content
    """
    import polars as pl

    for file_obj in items:
        with file_obj.open() as file:
            # Determine column names
            if column_names:
                names = column_names
            else:
                # Count columns from first row
                first_row = pl.read_csv(file, has_header=False, n_rows=1)
                num_columns = len(first_row.columns)
                names = [f"unknown_col_{i}" for i in range(num_columns)]
                file.seek(0)  # Reset file pointer after reading first row

            kwargs: Dict[str, Any] = {
                **{"has_header": False, "columns": names, "batch_size": chunksize},
                **polars_kwargs,
            }

            df = pl.read_csv(file, **kwargs)
            yield df.to_dicts()


def read_jsonl(
    items: Iterator[FileItemDict], chunksize: int = 1000
) -> Iterator[TDataItems]:
    """JSONL reader using Polars.

    Args:
        chunksize (int, optional): The number of JSON lines to load and yield at once, defaults to 1000

    Returns:
        TDataItem: The file content
    """
    for file_obj in items:
        with file_obj.open() as f:
            lines_chunk = []
            for line in f:
                lines_chunk.append(json.loadb(line))
                if len(lines_chunk) >= chunksize:
                    yield lines_chunk
                    lines_chunk = []
        if lines_chunk:
            yield lines_chunk


def read_bson(
    items: Iterator[FileItemDict], chunksize: int = 1000
) -> Iterator[TDataItems]:
    """BSON reader using bson.decode_file_iter.

    Mirrors ``read_jsonl`` but streams BSON documents with ``bson.decode_file_iter``
    and normalizes BSON extended values (ObjectId, Decimal128, Binary, datetime,
    Timestamp, Regex) into dlt-serializable Python types before yielding. ``bson`` and
    the normalizer are imported lazily so no other reader pays for them.

    Args:
        chunksize (int, optional): The number of BSON documents to load and yield at once, defaults to 1000

    Returns:
        TDataItem: The file content
    """
    import bson
    from dlt.common.utils import map_nested_values_in_place

    from omniload.source.filesystem.format.bson_codec import convert_bson_objs

    for file_obj in items:
        with file_obj.open() as f:
            docs_chunk = []
            for doc in bson.decode_file_iter(f):
                docs_chunk.append(map_nested_values_in_place(convert_bson_objs, doc))
                if len(docs_chunk) >= chunksize:
                    yield docs_chunk
                    docs_chunk = []
            # Flush this file's remainder before the next file resets docs_chunk, so a
            # multi-file glob doesn't drop a partial final chunk.
            if docs_chunk:
                yield docs_chunk


def read_parquet(
    items: Iterator[FileItemDict],
    chunksize: int = 10,
) -> Iterator[TDataItems]:
    """Parquet reader using pyarrow.

    Args:
        chunksize (int, optional): The number of files to process at once, defaults to 10.

    Returns:
        TDataItem: The file content
    """
    from pyarrow import parquet as pq

    for file_obj in items:
        with file_obj.open() as f:
            parquet_file = pq.ParquetFile(f)
            for rows in parquet_file.iter_batches(batch_size=chunksize):
                yield rows.to_pylist()


def read_csv_duckdb(
    items: Iterator[FileItemDict],
    chunk_size: Optional[int] = DEFAULT_CHUNK_SIZE,
    use_pyarrow: bool = False,
    **duckdb_kwargs: Any,
) -> Iterator[TDataItems]:
    """CSV reader using DuckDB.

    Uses DuckDB engine to import and cast CSV data.

    Args:
        items (Iterator[FileItemDict]): CSV files to read.
        chunk_size (Optional[int]):
            The number of rows to read at once. Defaults to 5000.
        use_pyarrow (bool):
            Whether to use `pyarrow` to read the data and designate
            data schema. If set to False (by default), JSON is used.
        duckdb_kwargs (Dict):
            Additional keyword arguments to pass to the `read_csv()`.

    Returns:
        Iterable[TDataItem]: Data items, read from the given CSV files.
    """
    import duckdb

    helper = fetch_arrow if use_pyarrow else fetch_json

    for item in items:
        with item.open() as f:
            file_data = duckdb.from_csv_auto(f, **duckdb_kwargs)

            yield from helper(file_data, chunk_size)


if TYPE_CHECKING:

    class ReadersSource(DltSource):
        """This is a typing stub that provides docstrings and signatures to the resources in `readers" source"""

        @copy_sig(read_csv)
        def read_csv(self) -> DltResource: ...

        @copy_sig(read_csv_headless)
        def read_csv_headless(self) -> DltResource: ...

        @copy_sig(read_jsonl)
        def read_jsonl(self) -> DltResource: ...

        @copy_sig(read_bson)
        def read_bson(self) -> DltResource: ...

        @copy_sig(read_parquet)
        def read_parquet(self) -> DltResource: ...

        @copy_sig(read_csv_duckdb)
        def read_csv_duckdb(self) -> DltResource: ...

else:
    ReadersSource = DltSource
