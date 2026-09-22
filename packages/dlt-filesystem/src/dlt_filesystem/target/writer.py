"""Format writers for the local ``file://`` destination.

Every writer takes ``(path, rows)`` and emits one file. Output is UTF-8 whatever the
process locale is, because the readers decode as UTF-8 unconditionally (``json.loadb``
rejects anything else, and Polars defaults to it), so a locale-encoded export would not
read back on the machine that wrote it.
"""

import datetime
import decimal

from dlt_filesystem.source.error import MissingDecoderError


def _column_union(rows: list[dict]) -> list[str]:
    """Union of keys in first-seen order.

    dlt omits null keys per row, so a later row can carry a column the first row lacked.
    First-seen order preserves the source column order (rather than sorting), which is
    what an export is expected to look like.
    """
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    return fieldnames


def _frame(rows: list[dict]):
    """Build a Polars frame over every column any row carries.

    ``infer_schema_length=None`` reads every row rather than the first 100, because
    dlt omits null keys per row: a column can first appear anywhere in the load, and
    the default window would drop one that first appears past it. Polars fills the
    gaps with null and keeps first-seen key order, which is what ``_column_union``
    was built by hand to do for the writers that predate this.

    A load with no keys to infer from becomes a frame with no columns rather than an
    error. That is an empty load, and also a load whose rows are all null: dlt omits a
    null key rather than writing it, so every row arrives as ``{}`` and Polars refuses
    a frame with height but no width.
    """
    import polars as pl

    if not any(rows):
        return pl.DataFrame()
    frame = pl.from_dicts(rows, infer_schema_length=None)
    _refuse_rounded_numbers(frame, rows)
    return frame


#: Past this, a double no longer holds every integer: the gap between representable
#: values grows beyond one, so a conversion can land on a different number.
_EXACT_IN_A_DOUBLE = 2**53


def _holds_an_inexact_number(value) -> bool:
    """Whether a value carries a number a double cannot be trusted to hold."""
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, decimal.Decimal)):
        return abs(value) > _EXACT_IN_A_DOUBLE
    if isinstance(value, dict):
        return any(_holds_an_inexact_number(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_holds_an_inexact_number(item) for item in value)
    return False


def _reads_back_as(source, written) -> bool:
    """Whether the frame still holds the number that went into it."""
    try:
        return decimal.Decimal(str(written)) == decimal.Decimal(source)
    except (ArithmeticError, ValueError, TypeError):
        # Not a pair of numbers to compare, so not this check's business.
        return True


def _refuse_a_rounded_number(name: str, source, written) -> None:
    """Compare one value against what the frame made of it, position by position.

    Positional rather than by type, because a column's final type does not say what
    happened on the way to it. Polars widens a column across the whole load, and a
    later widening hides an earlier one: a list of a large integer beside a list of a
    float becomes a list of floats, and one more row of strings turns that into a list
    of strings, so nothing in the type it ends up with says a number was rounded. By
    key and by index also keeps an unrelated field out of it, where reading the column
    as a whole would refuse a struct whose integer and float are separate fields and
    both exact.
    """
    if source is None or isinstance(source, bool):
        return
    if isinstance(source, (int, decimal.Decimal)):
        if written is not None and not _reads_back_as(source, written):
            raise ValueError(
                f"Column '{name}' carries the number {source}, which this format "
                f"would write as {written}: the column holds a float, and a double "
                "does not represent that number exactly. Write this load to a JSON, "
                "JSONL or YAML destination, which keep the number as itself."
            )
        return
    if isinstance(source, dict) and isinstance(written, dict):
        for key, value in source.items():
            _refuse_a_rounded_number(name, value, written.get(key))
    elif (
        isinstance(source, (list, tuple))
        and isinstance(written, (list, tuple))
        and len(source) == len(written)
    ):
        for value, item in zip(source, written):
            _refuse_a_rounded_number(name, value, item)


def _refuse_rounded_numbers(frame, rows: list[dict]) -> None:
    """Refuse a load whose numbers Polars would write rounded.

    A column holding a number past a double's exact range alongside a float becomes a
    float column, and the number is written rounded. PyArrow refused this outright
    (``Integer value ... is outside of the range exactly representable``), so a load
    that used to stop with an error would otherwise now finish with a wrong number in
    it. dlt splits a scalar column of two types into variants, but keeps a nested one
    as a single JSON column, so a list or a struct is how this arrives.

    The scan runs only when the load carries a number that large, which is what keeps
    it off every other write.
    """
    if not any(
        _holds_an_inexact_number(value) for row in rows for value in row.values()
    ):
        return
    for name in frame.columns:
        for row, written in zip(rows, frame[name].to_list()):
            _refuse_a_rounded_number(name, row.get(name), written)


#: Types a CSV column cannot hold. A ``Decimal`` is here rather than left to Polars
#: because Polars stops at 128-bit decimals, where PyArrow reached for a 256-bit one:
#: a ``DECIMAL(50,2)`` column survived the replaced writer and would abort this one.
_UNSPELLABLE_IN_CSV = (dict, list, tuple, bytes, bytearray, decimal.Decimal)


def _spell_for_csv(value):
    """Spell a value CSV cannot hold the way the JSON writers spell it.

    The same rule ``_yaml_dumper`` follows for the types PyYAML refuses: ask dlt's own
    serializer. A nested document becomes its JSON text and ``bytes`` become the base64
    string ``.json`` and ``.jsonl`` already write for it, so one value reads the same
    whichever format the export names.
    """
    from dlt.common import json

    if value is None:
        return None
    spelled = json.loads(json.dumps(value))
    # A scalar dlt spells as a string (base64 bytes, a decimal keeping its scale) is
    # that string, not a quoted JSON document; anything structural keeps its JSON text.
    return spelled if isinstance(spelled, str) else json.dumps(spelled)


def _spell_rows_for_csv(rows: list[dict]) -> list[dict]:
    """Spell those values before Polars sees them, not after.

    Polars types a column across the whole load, so a nested value read back out of a
    frame is no longer the value dlt produced: two rows carrying different keys come
    back with each other's keys as null, a list of one large integer beside a list of
    one float comes back rounded through f64, and a list mixing types does not build
    at all. dlt keeps a nested column as one JSON column rather than splitting it into
    variants, so all three are reachable on the default load path.
    """
    if not any(
        isinstance(value, _UNSPELLABLE_IN_CSV) for row in rows for value in row.values()
    ):
        return rows
    return [
        {
            key: _spell_for_csv(value)
            if isinstance(value, _UNSPELLABLE_IN_CSV)
            else value
            for key, value in row.items()
        }
        for row in rows
    ]


def write_csv(path: str, rows: list[dict]) -> None:
    """CSV writer using Polars.

    ``line_terminator`` is CRLF because that is what this destination has always
    written (``csv.DictWriter`` defaults to it, as does RFC 4180) and Polars defaults
    to LF; the migration is about the column union, not about changing the bytes of
    every existing export.

    CSV is flat, so Polars refuses a struct, list or binary column outright where
    ``csv.DictWriter`` accepted one and wrote ``str()`` of it: ``{'a': 1}`` for a nested
    document and ``b'hi'`` for binary, neither valid JSON nor readable back. A nested
    source reaches this on the default load path, so those values are spelled rather
    than left to abort the export.
    """
    frame = _frame(_spell_rows_for_csv(rows))
    # A record whose every field is null is a blank line in a one-column file, and a
    # blank line is not a record to most readers, so the row is lost on the way back
    # in. The csv module quoted a lone empty field for exactly this reason. Quoting
    # the file is what reproduces that here: filling the null instead would mean
    # casting the column to text, which would change how a date or a float is spelled
    # in the one file that happens to carry a null.
    lone_null_column = frame.width == 1 and frame.null_count().row(0)[0]
    frame.write_csv(
        path,
        line_terminator="\r\n",
        quote_style="always" if lone_null_column else "necessary",
    )


def write_feather(path: str, rows: list[dict]) -> None:
    """Write rows as an Arrow IPC file, the container Feather V2 names, with PyArrow.

    ``pa.ipc.new_file`` rather than ``pyarrow.feather.write_feather``: the latter is
    deprecated as of pyarrow 24 and its own warning names this API. It writes V2, which
    is what ``read_feather`` reads and what every current Arrow implementation opens.

    PyArrow rather than Polars, which is where ``write_csv`` and ``write_parquet`` went,
    for the reason ``write_orc`` also stayed: for these two the Arrow schema *is* the
    file. ``DataFrame.write_ipc`` defaults to Polars' own newest representation and
    writes ``string_view`` and ``binary_view`` columns, which are an Arrow 15 feature
    rather than something every reader opens, and keeping them out means pinning
    ``compat_level`` on a call where forgetting it changes the file rather than raising.
    Parquet has its own type system and was insulated from that; this format is not.
    """
    import pyarrow as pa

    fieldnames = _column_union(rows)
    if rows and not fieldnames:
        raise ValueError(
            "Feather output requires at least one column for nonempty rows"
        )
    columns = {name: [row.get(name) for row in rows] for name in fieldnames}
    table = pa.table(columns)
    with pa.ipc.new_file(path, table.schema) as writer:
        writer.write_table(table)


def write_json(path: str, rows: list[dict]) -> None:
    """JSON writer emitting one array document.

    One document rather than one per row, because that is the shape ``read_json``
    expects: it parses the whole file as a single value and expands an array to one row
    per element. A stream of ``---``-free concatenated documents would only load through
    the line-delimited fallback, which is what ``.jsonl`` is for.
    """
    from dlt.common import json

    with open(path, "wb") as handle:
        json.dump(rows, handle)


def write_jsonl(path: str, rows: list[dict]) -> None:
    """JSONL writer using json.dumps"""
    # dlt's json handles datetime/Decimal/etc. that dlt may have produced; stdlib json
    # would choke on them.
    from dlt.common import json

    with open(path, "wb") as handle:
        for row in rows:
            handle.write(json.dumpb(row) + b"\n")


def write_orc(path: str, rows: list[dict]) -> None:
    """Write rows as an ORC file with PyArrow."""
    import pyarrow as pa
    from pyarrow import orc

    fieldnames = _column_union(rows)
    if rows and not fieldnames:
        raise ValueError("ORC output requires at least one column for nonempty rows")
    columns = {name: [row.get(name) for row in rows] for name in fieldnames}
    orc.write_table(pa.table(columns), path)


def _narrowed_for_parquet(dtype):
    """The same dtype with every 128-bit integer replaced by ``UInt64``.

    Recursive because dlt keeps a nested column as one JSON column, and a struct
    field is typed exactly as a column is: a lone ``2**64-1`` field infers
    ``Int128``, and narrowing it is what makes the file readable. A list of structs,
    which is the shape a repeated JSON object arrives as, infers
    ``List(Struct({...: Int128}))`` and narrows the same way.

    A bare list of integers is the one shape that does not. ``from_dicts`` infers
    ``List(UInt64)`` for one whose elements all fit, so such a list reaches 128-bit
    only by holding a value the cast then refuses, and walking it turns an
    unreadable file into a named error rather than into a narrower column.
    """
    import polars as pl

    if dtype in (pl.Int128, pl.UInt128):
        return pl.UInt64
    if isinstance(dtype, pl.Struct):
        return pl.Struct(
            {field.name: _narrowed_for_parquet(field.dtype) for field in dtype.fields}
        )
    if isinstance(dtype, pl.List):
        return pl.List(_narrowed_for_parquet(dtype.inner))
    return dtype


def _narrow_wide_integers(frame):
    """Cast 128-bit integer columns to ``UInt64``, refusing values that will not fit.

    Polars widens an integer past the ``Int64`` range to ``Int128``, and past 2**127
    to ``UInt128``. Parquet has no 128-bit integer type, so a column of either is
    written as an untyped 16-byte ``FIXED_LEN_BYTE_ARRAY``: PyArrow refuses to read
    the file back at all (``Integers with more than 64 bits not implemented``) and
    DuckDB opens it and hands back a ``BLOB``, so the export leaves carrying a value
    no consumer can spend and nothing says so.

    A value in ``0..2**64-1`` is exactly what a ``UBIGINT`` source column produces, and
    ``UInt64`` is a type every Parquet reader has, so those narrow and read back
    unchanged. Above ``2**63`` that band is where this writer is the alternative
    ``write_feather`` and ``write_orc`` are not: PyArrow's inference from a Python
    int stops at a signed 64-bit, so both refuse from there up. Past ``2**64-1``, or on
    any negative in a column another row widened, all three refuse; the strict cast
    names the offending values, and the column they sit in except inside a bare
    list, so the write fails where the operator can see it rather than at whoever
    opens the file.

    Here rather than in ``_frame``: ``write_csv`` builds on that too, and writes these
    values correctly as text today.
    """
    target = {
        name: _narrowed_for_parquet(dtype)
        for name, dtype in frame.schema.items()
        if _narrowed_for_parquet(dtype) != dtype
    }
    return frame.cast(target, strict=True) if target else frame


def write_parquet(path: str, rows: list[dict]) -> None:
    """Parquet writer using Polars.

    ``compression`` is Snappy because that is what this destination has always written
    (PyArrow's default) and Polars defaults to Zstd. Zstd is the smaller of the two and
    every current reader handles it, but a codec is a thing a consumer either supports
    or fails on, so it is named here rather than changed as a side effect of moving
    libraries.
    """
    _narrow_wide_integers(_frame(rows)).write_parquet(path, compression="snappy")


def write_vortex(path: str, rows: list[dict]) -> None:
    """Vortex writer.

    Zero rows are written as a zero-column table, since ``vortex.array`` cannot infer
    a schema from an empty list. A datetime carrying a fixed UTC offset is written as
    the same instant in UTC: Vortex resolves a timezone by name, has no entry for an
    offset such as ``+12:00``, and aborts with a Rust panic that ``except Exception``
    does not catch, after truncating the destination.
    """
    try:
        import pyarrow as pa
        import vortex as vx  # ty: ignore[unresolved-import,unused-ignore-comment]
        import vortex.io as vxio  # ty: ignore[unresolved-import,unused-ignore-comment]
    except ImportError as e:
        raise MissingDecoderError(
            "Writing Vortex files needs the vortex-data package, which requires "
            "Python 3.11 or newer. "
            "Install it with: pip install 'dlt-filesystem[vortex]'"
        ) from e

    data = vx.array(_utc_fixed_offsets(rows)) if rows else pa.table({})
    vxio.write(data, path)


def _utc_fixed_offsets(value):
    """Return ``value`` with every fixed-offset datetime converted to UTC.

    Walks dicts and lists, and builds new containers only where it has to, so the
    caller's rows are never modified. A named zone (``zoneinfo``) and a naive datetime
    are left alone, since Vortex handles both.
    """
    if isinstance(value, datetime.datetime):
        tz = value.tzinfo
        if isinstance(tz, datetime.timezone) and tz is not datetime.timezone.utc:
            return value.astimezone(datetime.timezone.utc)
        return value
    if isinstance(value, dict):
        converted = {key: _utc_fixed_offsets(item) for key, item in value.items()}
        return value if all(converted[k] is value[k] for k in value) else converted
    if isinstance(value, list):
        converted = [_utc_fixed_offsets(item) for item in value]
        return value if all(a is b for a, b in zip(converted, value)) else converted
    return value


def write_yaml(path: str, rows: list[dict]) -> None:
    """YAML writer emitting one sequence document.

    One document holding a list, not a ``---``-separated stream of one document per
    row: ``read_yaml`` expands a list document to one row per element and yields any
    other document as a single row, so both shapes round-trip, and the list is the one
    that reads as a table rather than as a concatenation.

    ``sort_keys=False`` keeps each row in the column order the load produced, matching
    the other writers. ``allow_unicode=True`` writes non-ASCII as itself rather than as
    a ``\\xNN`` escape; the file is UTF-8 either way, but escaped output would be a
    gratuitous difference from what every other writer here emits.
    """
    try:
        import yaml
    except ImportError as e:
        raise MissingDecoderError(
            "Writing YAML needs the PyYAML package. "
            "Install it with: pip install 'dlt-filesystem[iterable]'"
        ) from e

    with open(path, "wb") as handle:
        yaml.dump(
            rows,
            handle,
            Dumper=_yaml_dumper(yaml),
            encoding="utf-8",
            allow_unicode=True,
            sort_keys=False,
        )


def _yaml_dumper(yaml_module) -> type:
    """``SafeDumper`` plus a fallback for the types dlt produces and YAML does not know.

    ``SafeDumper`` covers strings, numbers, booleans, null, sequences and mappings, and
    also ``bytes`` (as ``!!binary``) and ``date`` / ``datetime`` (as timestamps); it
    raises ``RepresenterError`` on anything else. That is the whole vocabulary of the
    default load path, where dlt stages gzip-JSONL and every value reaches a writer
    already JSON-typed. It is not the vocabulary of ``--loader-file-format parquet``,
    where a decimal column arrives as ``Decimal`` and a time column as
    ``datetime.time``, and an export would abort after the entire load had run.

    So an unknown type is spelled the way ``write_json`` and ``write_jsonl`` spell it,
    by asking dlt's own serializer: a ``Decimal`` writes as the string ``'1.50'``,
    keeping the scale a float would drop. The types YAML does know are left to
    ``SafeDumper``, so a timestamp still reads back as a datetime rather than as text.
    """

    class Dumper(yaml_module.SafeDumper):
        pass

    def represent_via_dlt_json(dumper, data):
        from dlt.common import json

        return dumper.represent_data(json.loads(json.dumps(data)))

    # ``None`` is PyYAML's key for the catch-all representer, the one that would
    # otherwise raise ``RepresenterError``.
    Dumper.add_representer(None, represent_via_dlt_json)
    return Dumper
