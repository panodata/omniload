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

import codecs
import io
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Union,
)

import dlt
from dlt.common import json
from dlt.common.typing import copy_sig
from dlt.sources import DltResource, DltSource, TDataItems
from dlt.sources.filesystem import FileItemDict

from dlt_filesystem.source.error import (
    MissingDecoderError,
    WorksheetNameCollisionError,
    _safe_location,
)
from dlt_filesystem.source.format.helpers import fetch_arrow, fetch_json
from dlt_filesystem.source.format.iterable_codec import read_via_iterable
from dlt_filesystem.source.format.settings import DEFAULT_CHUNK_SIZE
from dlt_filesystem.util.python import asbool, cast_kwargs_to_signature, is_polars_2


def _polars_csv_symbols() -> Dict[str, Any]:
    """Symbols needed to resolve `polars.read_csv`'s type hints for casting reader hints."""
    from typing import Callable, Mapping

    from polars import CredentialProvider, CredentialProviderFunction, DataFrame
    from polars._typing import (  # noqa: F401
        CsvEncoding,
        PolarsDataType,
        SchemaDict,
        StorageOptionsDict,
    )
    from polars.datatypes import DataType, DataTypeClass  # noqa: F401

    return {
        "CredentialProvider": CredentialProvider,
        "CredentialProviderFunction": CredentialProviderFunction,
        "CsvEncoding": CsvEncoding,
        "PolarsDataType": PolarsDataType,
        "SchemaDict": SchemaDict,
        "StorageOptionsDict": StorageOptionsDict,
        "DataType": DataType,
        "DataTypeClass": DataTypeClass,
        "Callable": Callable,
        "Mapping": Mapping,
        "DataFrame": DataFrame,
    }


def _polars_spreadsheet_symbols() -> Dict[str, Any]:
    """Symbols needed to cast reader hint values for `polars.read_excel` and `polars.read_ods`."""
    from typing import Sequence

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
        # On Python <3.14, evaluating Polars' ``memoryview[int]`` annotation
        # raises because the built-in is not yet subscriptable. A typing alias
        # preserves the intended bytes-like shape and lets the remaining reader
        # hints be resolved and cast.
        "memoryview": Sequence,
        "Sequence": Sequence,
    }


def spreadsheet_selection_is_plural(hints: Mapping[str, Any]) -> bool:
    """Return whether spreadsheet reader hints select worksheet tables."""

    if hints.get("table_name"):
        return False

    sheet_name = hints.get("sheet_name")
    if sheet_name not in (None, ""):
        if isinstance(sheet_name, (list, tuple)):
            return True
        if isinstance(sheet_name, str):
            try:
                return isinstance(json.loads(sheet_name), list)
            except (ValueError, TypeError):
                pass
        return False

    sheet_id = hints.get("sheet_id")
    if sheet_id not in (None, ""):
        if isinstance(sheet_id, (list, tuple)):
            return True
        if isinstance(sheet_id, str):
            try:
                sheet_id = json.loads(sheet_id)
            except (ValueError, TypeError):
                return False
        return sheet_id == 0 or isinstance(sheet_id, list)

    return True


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

    # Apply defaults.
    kwargs.setdefault("batch_size", chunksize)

    # pl.read_csv is now dispatched to pl.scan_csv(...).collect().
    # It loses n_threads, batch_size, sample_size, and rechunk,
    # which have no equivalent in the lazy reader.
    # https://docs.pola.rs/releases/upgrade/2/#plread_csv-is-now-dispatched-to-plscan_csvcollect
    if is_polars_2():
        kwargs.pop("batch_size", None)

    import polars as pl

    kwargs = cast_kwargs_to_signature(
        pl.read_csv, kwargs, symbols=_polars_csv_symbols()
    )

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
                # `new_columns` names the columns; `columns` *selects* them, and
                # selecting by name from a file that has no header asks Polars for
                # names it generated itself, which it rejects outright on some
                # inputs and answers with shifted values on others.
                **{"has_header": False, "new_columns": names, "batch_size": chunksize},
                **polars_kwargs,
            }

            # pl.read_csv is now dispatched to pl.scan_csv(...).collect().
            # It loses n_threads, batch_size, sample_size, and rechunk,
            # which have no equivalent in the lazy reader.
            # https://docs.pola.rs/releases/upgrade/2/#plread_csv-is-now-dispatched-to-plscan_csvcollect
            if is_polars_2():
                kwargs.pop("batch_size", None)

            df = pl.read_csv(file, **kwargs)
            yield df.to_dicts()


def read_excel(
    items: Iterator[FileItemDict],
    worksheet_names: Optional[dict[str, tuple[str, str]]] = None,
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

    yield from read_spreadsheet(
        reader=pl.read_excel,
        items=items,
        worksheet_names=worksheet_names,
        **kwargs,
    )


def read_ods(
    items: Iterator[FileItemDict],
    worksheet_names: Optional[dict[str, tuple[str, str]]] = None,
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

    yield from read_spreadsheet(
        reader=pl.read_ods,
        items=items,
        worksheet_names=worksheet_names,
        **kwargs,
    )


def _file_location(file_obj: FileItemDict, fallback: str) -> str:
    """Name a file for an error message, with any credentials in the URL removed.

    A listed item carries the three names in preference order: the full URL, the path
    relative to the bucket, and the bare file name. Which of them is populated depends
    on the transport, so all three are tried rather than one being assumed.
    """
    return _safe_location(
        str(
            file_obj.get("file_url")
            or file_obj.get("relative_path")
            or file_obj.get("file_name")
            or fallback
        )
    )


def read_spreadsheet(
    reader: Callable,
    items: Iterator[FileItemDict],
    worksheet_names: Optional[dict[str, tuple[str, str]]] = None,
    **kwargs,
) -> Iterator[TDataItems]:
    """Universal reader for ODS and XLSX spreadsheet / workbook files."""

    if "sheet_name" in kwargs and not kwargs["sheet_name"]:
        kwargs.pop("sheet_name")

    plural_selection = spreadsheet_selection_is_plural(kwargs)
    kwargs = cast_kwargs_to_signature(
        reader, kwargs, symbols=_polars_spreadsheet_symbols()
    )
    if plural_selection:
        if not any(
            selector in kwargs and kwargs[selector] is not None
            for selector in ("sheet_id", "sheet_name", "table_name")
        ):
            kwargs["sheet_id"] = 0
        # Blank worksheets are common and cannot produce a dlt table without an
        # explicit schema. Read them as empty frames and skip them below.
        kwargs.setdefault("raise_if_empty", False)

    seen_table_names = worksheet_names if worksheet_names is not None else {}
    for file_obj in items:
        with file_obj.open() as f:
            workbook = reader(f.read(), **kwargs)

        if not isinstance(workbook, dict):
            yield workbook.rows(named=True)
            continue

        file_location = _file_location(file_obj, "<unknown workbook>")
        naming = dlt.current.source_schema().naming
        for sheet_name, frame in workbook.items():
            rows = frame.rows(named=True)
            if not rows:
                continue

            normalized_name = naming.normalize_table_identifier(sheet_name)
            previous = seen_table_names.get(normalized_name)
            if previous is not None and previous[0] != sheet_name:
                raise WorksheetNameCollisionError(
                    table_name=normalized_name,
                    first_sheet=previous[0],
                    first_file=previous[1],
                    second_sheet=sheet_name,
                    second_file=file_location,
                )
            seen_table_names[normalized_name] = (sheet_name, file_location)
            yield dlt.mark.with_table_name(rows, sheet_name)


def _validated_chunksize(chunksize: Any) -> int:
    """Coerce a ``#chunksize=`` reader hint to a positive integer.

    Hints arrive as strings, so the cast is the validation: a non-numeric value is a
    ``TypeError`` naming what was given, and a non-positive one a ``ValueError``, rather
    than an empty read or an infinite loop further down.
    """
    try:
        chunksize = int(chunksize)
    except (TypeError, ValueError):
        raise TypeError(f"chunksize must be an integer, not {chunksize}")
    if chunksize < 1:
        raise ValueError(f"chunksize must be greater than zero, not {chunksize}")
    return chunksize


def _decode_column_selection(
    columns: Optional[Union[list[str], str]],
) -> Optional[list[str]]:
    """Decode a ``#columns=`` reader hint into a list of column names.

    A hint arrives as a string in both of its spellings: the JSON list a caller writes
    for several columns (``#columns=["id","name"]``) and the bare name for one
    (``#columns=id``). A value that is already a list is passed through, which is how a
    Python caller supplies it.
    """
    if not isinstance(columns, str):
        return columns
    try:
        decoded = json.loads(columns)
    except (ValueError, TypeError):
        return [columns]
    return decoded if isinstance(decoded, list) else [decoded]


def read_orc(
    items: Iterator[FileItemDict],
    chunksize: int = 1000,
    columns: Optional[Union[list[str], str]] = None,
) -> Iterator[TDataItems]:
    """Reader for ORC files that yields chunked stripe output."""
    from pyarrow import orc

    chunksize = _validated_chunksize(chunksize)
    # `columns` applies to `read_stripe`, not the ORCFile constructor.
    columns = _decode_column_selection(columns)

    for file_obj in items:
        with file_obj.open() as f:
            orc_file = orc.ORCFile(f)
            for stripe_index in range(orc_file.nstripes):
                stripe = orc_file.read_stripe(stripe_index, columns=columns)
                for offset in range(0, stripe.num_rows, chunksize):
                    yield stripe.slice(offset, chunksize).to_pylist()


#: Feather V1's container magic. V2 is the Arrow IPC file format, whose magic is
#: ``ARROW1``; V1 is a different container that only the deprecated ``pyarrow.feather``
#: reader opens, so it is detected here and named rather than left to fail as corruption.
_FEATHER_V1_MAGIC = b"FEA1"


def _reject_feather_v1(handle: Any) -> None:
    """Refuse a Feather V1 file by its own magic rather than by what V2 fails to find.

    ``pa.ipc.open_file`` reports ``Not an Arrow file`` for a V1 file, which reads as "this
    file is damaged" for a file that is a perfectly good Feather V1. Only the deprecated
    ``pyarrow.feather`` reader opens that container.
    """
    magic = handle.read(len(_FEATHER_V1_MAGIC))
    handle.seek(0)
    if magic == _FEATHER_V1_MAGIC:
        raise ValueError(
            "Feather V1 files are not supported; this reader handles Feather V2, "
            "the Arrow IPC file format. Rewrite the file as V2 to read it."
        )


def _reject_unknown_columns(selection: list[str], available: list[str]) -> None:
    """Reject a ``#columns=`` selection naming a column the file does not carry.

    ``RecordBatch.select`` raises a bare ``KeyError`` that does not say what the valid
    names are. The wording is ORC's, which pyarrow produces for the same mistake on
    ``read_stripe``, so the two columnar readers answer a typo the same way.
    """
    for name in selection:
        if name not in available:
            raise ValueError(
                f"Invalid column selected {name}. "
                f"Valid names are {', '.join(sorted(available))}"
            )


def read_feather(
    items: Iterator[FileItemDict],
    chunksize: int = 1000,
    columns: Optional[Union[list[str], str]] = None,
) -> Iterator[TDataItems]:
    """Reader for Feather V2 (Arrow IPC file) data that yields chunked batch output.

    Record batches are read one at a time through ``get_batch``, the Arrow IPC analogue
    of ORC's stripes, so a file written in batches is never materialized whole. A batch
    larger than ``chunksize`` is sliced; a smaller one yields as it stands, so physical
    batch boundaries are visible in the output.

    Args:
        chunksize (int, optional): The number of rows to yield at once, defaults to 1000.
        columns (optional): Columns to read, as a list or a ``#columns=`` hint.

    Returns:
        TDataItem: The file content
    """
    import pyarrow as pa

    chunksize = _validated_chunksize(chunksize)
    selection = _decode_column_selection(columns)

    for file_obj in items:
        with file_obj.open() as f:
            _reject_feather_v1(f)
            reader = pa.ipc.open_file(f)
            if selection is not None:
                _reject_unknown_columns(selection, reader.schema.names)
            for batch_index in range(reader.num_record_batches):
                batch = reader.get_batch(batch_index)
                if selection is not None:
                    batch = batch.select(selection)
                for offset in range(0, batch.num_rows, chunksize):
                    yield batch.slice(offset, chunksize).to_pylist()


def _avro_panic_message(location: str, panic: BaseException) -> str:
    """Word a panic raised out of ``pl.read_avro``, naming the file and the likely cause.

    ``pl.read_avro`` reads Avro into Arrow, and a type it cannot map aborts in Rust: a
    ``map`` field raises ``PanicException: Avro maps are mapped to MapArrays``. In any
    build that carries the compiled binary -- which is any build that can read Avro at
    all -- that class inherits ``BaseException`` directly, so nothing above this reader
    sees it (dlt's own extract step wraps ``Exception``) and the load would end in a Rust
    backtrace naming neither the file nor the field. A ``map`` is an ordinary Avro type,
    so this is reachable with a perfectly valid file.

    Two causes are known and neither is exotic, so both are named: an unmappable
    schema, and a timestamp outside Python's own range, which decodes into Polars
    quite happily and then panics on the way out to Python objects. The wording keeps
    them as the likely causes rather than asserting either, because the handlers catch
    any panic out of those calls; the panic's own detail line carries the truth.
    """
    detail = (
        str(panic).strip().splitlines()[0]
        if str(panic).strip()
        else type(panic).__name__
    )
    return (
        f"Reading Avro file {location} aborted inside Polars: {detail}. Two causes are "
        "known: a schema with no Arrow mapping (an Avro `map` field is the one to look "
        "for, and rewriting it as a record avoids this), and a timestamp outside the "
        "year range Python's `datetime` can hold."
    )


def read_avro(
    items: Iterator[FileItemDict],
    chunksize: int = 1000,
    columns: Optional[Union[list[str], str]] = None,
) -> Iterator[TDataItems]:
    """Reader for Apache Avro object container files, using Polars.

    Whole-file rather than streamed: Polars has no ``scan_avro``, so ``chunksize``
    bounds what a downstream step is handed at once, not what is held in memory. That
    is the same shape ``read_excel`` and ``read_ods`` have and is stated on the format
    page rather than left to be discovered.

    Args:
        chunksize (int, optional): The number of rows to yield at once, defaults to 1000.
        columns (optional): Columns to read, as a list or a ``#columns=`` hint.

    Returns:
        TDataItem: The file content
    """
    import polars as pl

    chunksize = _validated_chunksize(chunksize)
    selection = _decode_column_selection(columns)

    for file_obj in items:
        location = _file_location(file_obj, "<unknown avro file>")
        with file_obj.open() as f:
            try:
                frame = pl.read_avro(f, columns=selection)
            except pl.exceptions.PanicException as e:
                raise ValueError(_avro_panic_message(location, e)) from e
        for offset in range(0, frame.height, chunksize):
            # Guarded separately from the read, because decoding and converting panic
            # for unrelated reasons and either one escapes on its own. A timestamp past
            # year 9999 lands in the frame without complaint and panics only here, on
            # the way out to Python objects.
            try:
                chunk = frame.slice(offset, chunksize).to_dicts()
            except pl.exceptions.PanicException as e:
                raise ValueError(_avro_panic_message(location, e)) from e
            yield chunk


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


def read_json(
    items: Iterator[FileItemDict], chunksize: int = 1000
) -> Iterator[TDataItems]:
    """JSON reader for a whole document, falling back to line-delimited records.

    A `.json` file carries any of three shapes in the wild, and the extension does not say
    which: one object, an array of objects, or line-delimited records that were named `.json`
    rather than `.jsonl`. The whole document is parsed first, so a pretty-printed array or
    object is read as what it is. Line-delimited parsing is the fallback, tried only when the
    document does not parse as one value, because a JSONL body's first line parses on its own
    and would otherwise be mistaken for a complete document.

    Deciding on line count instead would misread every pretty-printed file, and deciding on the
    first line alone would truncate every JSONL file to its first record.

    Args:
        chunksize (int, optional): The number of records to load and yield at once.

    Returns:
        TDataItem: The file content
    """
    for file_obj in items:
        with file_obj.open() as f:
            data = f.read()

        # A UTF-8 BOM is not part of the document and is not valid JSON, so a file exported
        # from a tool that writes one would otherwise fail on both the document and the
        # line-delimited path.
        if data.startswith(codecs.BOM_UTF8):
            data = data[len(codecs.BOM_UTF8) :]

        try:
            document = json.loadb(data)
        except ValueError:
            # Only a decode failure means "not one document"; the decoder raises a
            # `JSONDecodeError`, which is a `ValueError`. A `MemoryError` or a recursion
            # limit says the body is too big or too deep, and retrying it line by line
            # would relabel a resource failure as a parse failure.
            yield from _read_json_lines(data, chunksize)
            continue

        if isinstance(document, list):
            for start in range(0, len(document), chunksize):
                yield document[start : start + chunksize]
        else:
            yield [document]


def _read_json_lines(data: bytes, chunksize: int) -> Iterator[TDataItems]:
    """Yield records from a line-delimited JSON body already read into memory.

    Blank lines are skipped, so a trailing newline does not raise. Any other malformed line
    raises, naming the line number, because silently dropping a record is worse than failing
    the load.

    The body is iterated as a stream rather than split into a list, so the lines are not all
    materialized at once on top of the body this already holds. An empty body yields nothing,
    which is the same zero-row load an empty array gives.
    """
    chunk: List[Any] = []
    for number, line in enumerate(io.BytesIO(data), start=1):
        if not line.strip():
            continue
        try:
            chunk.append(json.loadb(line))
        except ValueError as ex:
            raise ValueError(
                f"JSON document is neither a single value nor line-delimited records: "
                f"line {number} does not parse ({ex})"
            ) from ex
        if len(chunk) >= chunksize:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


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

    from dlt_filesystem.source.format.bson_codec import convert_bson_objs

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


def read_vortex(
    items: Iterator[FileItemDict],
    chunksize: int = 5000,
) -> Iterator[TDataItems]:
    """Vortex reader.

    The file is scanned in batches of ``chunksize`` rows, and no chunk handed downstream
    is larger than that.

    Args:
        chunksize (int, optional): The number of rows to yield at once. Defaults to 5000.

    Returns:
        TDataItem: The file content
    """
    chunksize = _validated_chunksize(chunksize)
    vx = _import_vortex("Reading")

    for file_obj in items:
        with _vortex_local_path(file_obj) as path:
            for batch in vx.open(path).scan(batch_size=chunksize).to_arrow():
                for offset in range(0, batch.num_rows, chunksize):
                    yield batch.slice(offset, chunksize).to_pylist()


def _import_vortex(action: str) -> Any:
    """Import ``vortex``, or raise the install hint the other optional formats give."""
    try:
        import vortex  # ty: ignore[unresolved-import,unused-ignore-comment]
    except ImportError as e:
        raise MissingDecoderError(
            f"{action} Vortex files needs the vortex-data package, which requires "
            "Python 3.11 or newer. Install it with: pip install 'dlt-filesystem[vortex]'"
        ) from e
    return vortex


@contextmanager
def _vortex_local_path(file_obj: Any) -> Iterator[str]:
    """Yield a local path for ``file_obj`` that ``vortex.open`` can read.

    ``vortex.open`` takes a path string only, not a file handle. A plain local file is
    read in place. Anything else, a remote object or a gzipped file on any filesystem,
    is copied through the item's own ``open()`` into a temporary file first, which is
    what decompresses it and what reuses the source's authentication. The source handle
    is closed once the copy is done.

    "Local" is decided by the item's ``file_url`` scheme, which is what
    ``FileItemDict.local_file_path`` converts, rather than by the filesystem's
    protocol: the local source's wrapper reports ``local``, not ``file``.
    """
    if (
        isinstance(file_obj, FileItemDict)
        and str(file_obj.get("file_url", "")).startswith("file://")
        and file_obj.get("encoding") != "gzip"
    ):
        yield file_obj.local_file_path
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "staged.vortex"
        if isinstance(file_obj, FileItemDict):
            source = file_obj.open(compression="auto")
        else:
            source = file_obj.open()
        with source as source_file, path.open("wb") as staged_file:
            shutil.copyfileobj(source_file, staged_file)
        yield str(path)


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

    parsed_use_pyarrow = False if use_pyarrow == "" else asbool(use_pyarrow)
    helper = fetch_arrow if parsed_use_pyarrow else fetch_json

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

        @copy_sig(read_json)
        def read_json(self) -> DltResource:
            """JSON reader resource (whole document or line-delimited)."""

        @copy_sig(read_jsonl)
        def read_jsonl(self) -> DltResource:
            """JSONL reader resource."""

        @copy_sig(read_bson)
        def read_bson(self) -> DltResource:
            """BSON reader resource."""

        @copy_sig(read_msgpack)
        def read_msgpack(self) -> DltResource:
            """MessagePack reader resource."""

        @copy_sig(read_orc)
        def read_orc(self) -> DltResource:
            """ORC reader resource (pyarrow)."""

        @copy_sig(read_feather)
        def read_feather(self) -> DltResource:
            """Feather V2 / Arrow IPC reader resource (pyarrow)."""

        @copy_sig(read_avro)
        def read_avro(self) -> DltResource:
            """Apache Avro reader resource (Polars)."""

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

        @copy_sig(read_vortex)
        def read_vortex(self) -> DltResource:
            """Vortex reader resource (vortex-data)."""

        @copy_sig(read_csv_duckdb)
        def read_csv_duckdb(self) -> DltResource:
            """CSV reader resource (DuckDB)."""

else:
    ReadersSource = DltSource
