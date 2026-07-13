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

from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional

from dlt.common import json
from dlt.common.typing import copy_sig
from dlt.sources import DltResource, DltSource, TDataItems
from dlt.sources.filesystem import FileItemDict

from omniload.codec.python import cast_kwargs_to_signature
from omniload.source.filesystem.format.helpers import fetch_arrow, fetch_json
from omniload.source.filesystem.format.iterable_codec import read_via_iterable
from omniload.source.filesystem.format.settings import DEFAULT_CHUNK_SIZE


def _polars_csv_symbols() -> Dict[str, Any]:
    """Symbols needed to resolve `polars.read_csv`'s type hints for casting reader hints."""
    from typing import Mapping

    from polars import DataFrame
    from polars._typing import (  # noqa: F401
        CsvEncoding,
        PolarsDataType,
        SchemaDict,
        StorageOptionsDict,
    )
    from polars.datatypes import DataType, DataTypeClass  # noqa: F401

    return {
        "CsvEncoding": CsvEncoding,
        "PolarsDataType": PolarsDataType,
        "SchemaDict": SchemaDict,
        "StorageOptionsDict": StorageOptionsDict,
        "DataType": DataType,
        "DataTypeClass": DataTypeClass,
        "Mapping": Mapping,
        "DataFrame": DataFrame,
    }


def _polars_spreadsheet_symbols() -> Dict[str, Any]:
    """Symbols needed to cast reader hint values for `polars.read_excel` and `polars.read_ods`."""
    from polars._typing import (  # noqa: F401
        ExcelSpreadsheetEngine,
        FileSource,
        SchemaDict,
    )
    from polars.datatypes import DataType, DataTypeClass  # noqa: F401

    return {
        "ExcelSpreadsheetEngine": ExcelSpreadsheetEngine,
        "FileSource": FileSource,
        "SchemaDict": SchemaDict,
        "DataType": DataType,
        "DataTypeClass": DataTypeClass,
    }


def read_csv(
    items: Iterator[FileItemDict], chunksize: int = 10000, **kwargs: Any
) -> Iterator[TDataItems]:
    """CSV reader using Polars.

    Args:
        chunksize (int): Number of records to read in one chunk
        **polars_kwargs: Additional keyword arguments passed to polars.read_csv
    Returns:
        TDataItem: The file content
    """
    import polars as pl

    kwargs = cast_kwargs_to_signature(
        pl.read_csv, kwargs, symbols=_polars_csv_symbols()
    )

    # Apply defaults.
    kwargs.setdefault("batch_size", chunksize)

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

    polars_kwargs = cast_kwargs_to_signature(
        pl.read_csv, polars_kwargs, symbols=_polars_csv_symbols()
    )

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


def read_excel(
    items: Iterator[FileItemDict],
    **kwargs,
) -> Iterator[TDataItems]:
    """
    Read XLSX file content and extract the data.

    Parameters
    ----------

    sheet_id
        Sheet number(s) to convert (set `0` to load all sheets as DataFrames) and
        return a `{sheetname:frame,}` dict. (Defaults to `1` if neither this nor
        `sheet_name` are specified). Can also take a sequence of sheet numbers.
    sheet_name
        Sheet name(s) to convert; cannot be used in conjunction with `sheet_id`. If
        more than one is given then a `{sheetname:frame,}` dict is returned.
    table_name
        Name of a specific table to read; note that table names are unique across
        the workbook, so additionally specifying a sheet id or name is optional;
        if one of those parameters *is* specified, an error will be raised if
        the named table is not found in that particular sheet.
    engine : {'calamine', 'openpyxl', 'xlsx2csv'}
        Library used to parse the spreadsheet file; defaults to "calamine".

        * "calamine": this engine can be used for reading all major types of Excel
          Workbook (`.xlsx`, `.xlsb`, `.xls`) and is dramatically faster than the
          other options, using the `fastexcel` module to bind the Rust-based Calamine
          parser.
        * "openpyxl": this engine is significantly slower than both `calamine` and
          `xlsx2csv`, but can provide a useful fallback if you are otherwise unable
          to read data from your workbook.
        * "xlsx2csv": converts the data to an in-memory CSV before using the native
          polars `read_csv` method to parse the result.
    engine_options
        Additional options passed to the underlying engine's primary parsing
        constructor (given below), if supported:

        * "calamine": n/a (can only provide `read_options`)
        * "openpyxl": `load_workbook <https://openpyxl.readthedocs.io/en/stable/api/openpyxl.reader.excel.html#openpyxl.reader.excel.load_workbook>`_
        * "xlsx2csv": `Xlsx2csv <https://github.com/dilshod/xlsx2csv/blob/f35734aa453d65102198a77e7b8cd04928e6b3a2/xlsx2csv.py#L157>`_
    read_options
        Options passed to the underlying engine method that reads the sheet data.
        Where supported, this allows for additional control over parsing. The
        specific read methods associated with each engine are:

        * "calamine": `load_sheet_by_name <https://fastexcel.toucantoco.dev/fastexcel.html#ExcelReader.load_sheet_by_name>`_
          (or `load_table <https://fastexcel.toucantoco.dev/fastexcel.html#ExcelReader.load_table>`_
          if using the `table_name` parameter).
        * "openpyxl": n/a (can only provide `engine_options`)
        * "xlsx2csv": see :meth:`read_csv`
    has_header
        Indicate if the first row of the table data is a header or not. If False,
        column names will be autogenerated in the following format: `column_x`, with
        `x` being an enumeration over every column in the dataset, starting at 1.
    columns
        Columns to read from the sheet; if not specified, all columns are read. Can
        be given as a sequence of column names or indices, or a single column name.
    schema_overrides
        Support type specification or override of one or more columns.
    infer_schema_length
        The maximum number of rows to scan for schema inference. If set to `None`, the
        entire dataset is scanned to determine the dtypes, which can slow parsing for
        large workbooks. Note that only the "calamine" and "xlsx2csv" engines support
        this parameter.
    include_file_paths
        Include the path of the source file(s) as a column with this name.
    drop_empty_rows
        Indicate whether to omit empty rows when reading data into the DataFrame.
    drop_empty_cols
        Indicate whether to omit empty columns (with no headers) when reading data into
        the DataFrame (note that empty column identification may vary depending on the
        underlying engine being used).
    raise_if_empty
        When there is no data in the sheet,`NoDataError` is raised. If this parameter
        is set to False, an empty DataFrame (with no columns) is returned instead.

    Returns
    -------

    TDataItem
        The file content
    """
    import polars as pl

    yield from read_spreadsheet(reader=pl.read_excel, items=items, **kwargs)


def read_ods(
    items: Iterator[FileItemDict],
    **kwargs,
) -> Iterator[TDataItems]:
    """
    Read OpenOffice (ODS) spreadsheet content and extract the data.

    Parameters
    ----------

    source
        Path to a file or a file-like object (by "file-like object" we refer to objects
        that have a `read()` method, such as a file handler like the builtin `open`
        function, or a `BytesIO` instance). For file-like objects, the stream position
        may not be updated accordingly after reading.
    sheet_id
        Sheet number(s) to convert, starting from 1 (set `0` to load *all* worksheets
        as DataFrames) and return a `{sheetname:frame,}` dict. (Defaults to `1` if
        neither this nor `sheet_name` are specified). Can also take a sequence of sheet
        numbers.
    sheet_name
        Sheet name(s) to convert; cannot be used in conjunction with `sheet_id`. If
        more than one is given then a `{sheetname:frame,}` dict is returned.
    has_header
        Indicate if the first row of the table data is a header or not. If False,
        column names will be autogenerated in the following format: `column_x`, with
        `x` being an enumeration over every column in the dataset, starting at 1.
    columns
        Columns to read from the sheet; if not specified, all columns are read. Can
        be given as a sequence of column names or indices.
    schema_overrides
        Support type specification or override of one or more columns.
    infer_schema_length
        The maximum number of rows to scan for schema inference. If set to `None`, the
        entire dataset is scanned to determine the dtypes, which can slow parsing for
        large workbooks.
    include_file_paths
        Include the path of the source file(s) as a column with this name.
    drop_empty_rows
        Indicate whether to omit empty rows when reading data into the DataFrame.
    drop_empty_cols
        Indicate whether to omit empty columns (with no headers) when reading data into
        the DataFrame (note that empty column identification may vary depending on the
        underlying engine being used).
    raise_if_empty
        When there is no data in the sheet,`NoDataError` is raised. If this parameter
        is set to False, an empty DataFrame (with no columns) is returned instead.

    Returns
    -------

    TDataItem
        The file content
    """
    import polars as pl

    yield from read_spreadsheet(reader=pl.read_ods, items=items, **kwargs)


def read_spreadsheet(
    reader: Callable,
    items: Iterator[FileItemDict],
    **kwargs,
) -> Iterator[TDataItems]:
    """Universal reader for ODS and XLSX spreadsheet / workbook files."""

    if "sheet_name" in kwargs and not kwargs["sheet_name"]:
        kwargs["sheet_name"] = None

    kwargs = cast_kwargs_to_signature(
        reader, kwargs, symbols=_polars_spreadsheet_symbols()
    )

    for file_obj in items:
        with file_obj.open() as f:
            yield reader(f.read(), **kwargs).rows(named=True)


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


def read_msgpack(
    items: Iterator[FileItemDict], chunksize: int = 1000
) -> Iterator[TDataItems]:
    """MessagePack reader backed by iterabledata's ``MessagePackIterable``.

    Thin wrapper over the generic ``read_via_iterable`` harness (see
    ``format.iterable_codec``): streams the fsspec handle into the iterabledata class and
    normalizes msgpack ``bytes`` / ``Timestamp`` values to dlt-safe types. ``iterable`` and
    ``msgpack`` are imported lazily inside the harness, so no other reader pays for them.

    Args:
        chunksize (int, optional): The number of records to load and yield at once, defaults to 1000.

    Returns:
        TDataItem: The file content
    """
    yield from read_via_iterable(items, file_format="msgpack", chunksize=chunksize)


def read_cbor(
    items: Iterator[FileItemDict], chunksize: int = 1000
) -> Iterator[TDataItems]:
    """CBOR reader (decoded with cbor2 directly through the generic harness).

    Thin wrapper over ``read_via_iterable`` (see ``format.iterable_codec``). CBOR is whole-file
    and iterabledata's ``CBORIterable`` swallows decode errors, so the harness decodes with
    ``cbor2`` directly (surfacing corrupt/truncated files) and normalizes CBOR ``bytes``
    (base64) and unknown ``CBORTag`` values to dlt-safe types. ``cbor2`` is imported lazily.

    The source must be a **single top-level CBOR value**: an array yields one row per element
    and a single map yields one row. Files that concatenate several top-level CBOR objects are
    read only up to the first (a cbor2 limitation that cannot be detected), so write a
    top-level array instead. See ``docs/supported-sources/cbor.md``.

    Args:
        chunksize (int, optional): The number of records to load and yield at once, defaults to 1000.

    Returns:
        TDataItem: The file content
    """
    yield from read_via_iterable(items, file_format="cbor", chunksize=chunksize)


def read_xml(
    items: Iterator[FileItemDict], chunksize: int = 1000, **options: Any
) -> Iterator[TDataItems]:
    """XML reader (parsed with a hardened lxml parser through the generic harness).

    Thin wrapper over ``read_via_iterable`` (see ``format.iterable_codec``). XML is whole-file
    and iterabledata's XML parser resolves entities and can't be locked down through its API, so
    the harness parses with ``lxml`` directly under a safe configuration (no entity resolution,
    no DTD load, no network, capped tree) that neutralizes XXE / entity-expansion attacks.

    A ``tagname`` option is **required**: it names the repeated element that is one row and
    arrives via the ``#tagname=<row-tag>`` URI fragment (the first consumer of the reader-hint
    channel). Without it the reader raises a clear ``MissingReaderOptionError``, never a bare
    ``AttributeError``. Each row element becomes a record (attributes under ``@name``, repeated
    children as lists); see ``docs/supported-sources/xml.md``. ``lxml`` is imported lazily.

    Args:
        chunksize (int, optional): The number of records to load and yield at once, defaults to 1000.
        **options: Reader hints forwarded to the decoder (``tagname`` for XML).

    Returns:
        TDataItem: The file content
    """
    yield from read_via_iterable(
        items, file_format="xml", chunksize=chunksize, **options
    )


def read_yaml(
    items: Iterator[FileItemDict], chunksize: int = 1000, **options: Any
) -> Iterator[TDataItems]:
    """YAML reader (parsed with ``yaml.safe_load_all`` through the generic harness).

    Thin wrapper over ``read_via_iterable`` (see ``format.iterable_codec``). YAML is whole-file
    and iterabledata's YAML wrapper is eager and swallows parse errors, so the harness loads
    with ``yaml.safe_load_all`` directly: a ``!!python/object`` tag is rejected (never executed)
    and a malformed document raises instead of silently loading zero rows. Each YAML document
    becomes rows -- a top-level list expands to one row per element, any other document is one
    row -- and ``!!binary`` / ``!!set`` leaves are normalized to dlt-safe types. ``yaml`` is
    imported lazily. See ``docs/supported-sources/yaml.md``.

    Args:
        chunksize (int, optional): The number of records to load and yield at once, defaults to 1000.
        **options: Reader hints forwarded to the decoder (YAML takes none).

    Returns:
        TDataItem: The file content
    """
    yield from read_via_iterable(
        items, file_format="yaml", chunksize=chunksize, **options
    )


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
        def read_csv(self) -> DltResource:
            """CSV reader resource (Polars)."""

        @copy_sig(read_csv_headless)
        def read_csv_headless(self) -> DltResource:
            """Header-less CSV reader resource (Polars)."""

        @copy_sig(read_excel)
        def read_excel(self) -> DltResource:
            """XLSX reader resource (Polars)."""

        @copy_sig(read_excel)
        def read_ods(self) -> DltResource:
            """ODS reader resource (Polars)."""

        @copy_sig(read_jsonl)
        def read_jsonl(self) -> DltResource:
            """JSONL reader resource."""

        @copy_sig(read_bson)
        def read_bson(self) -> DltResource:
            """BSON reader resource."""

        @copy_sig(read_msgpack)
        def read_msgpack(self) -> DltResource:
            """MessagePack reader resource."""

        @copy_sig(read_cbor)
        def read_cbor(self) -> DltResource:
            """CBOR reader resource."""

        @copy_sig(read_xml)
        def read_xml(self) -> DltResource:
            """XML reader resource."""

        @copy_sig(read_yaml)
        def read_yaml(self) -> DltResource:
            """YAML reader resource."""

        @copy_sig(read_parquet)
        def read_parquet(self) -> DltResource:
            """Parquet reader resource (pyarrow)."""

        @copy_sig(read_csv_duckdb)
        def read_csv_duckdb(self) -> DltResource:
            """CSV reader resource (DuckDB)."""

else:
    ReadersSource = DltSource
