"""Format writers for the local ``file://`` destination.

Every writer takes ``(path, rows)`` and emits one file. Output is UTF-8 whatever the
process locale is, because the readers decode as UTF-8 unconditionally (``json.loadb``
rejects anything else, and Polars defaults to it), so a locale-encoded export would not
read back on the machine that wrote it.
"""

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


def write_csv(path: str, rows: list[dict]) -> None:
    """CSV writer using csv.DictWriter"""
    import csv

    fieldnames = _column_union(rows)

    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, restval="")
        writer.writeheader()
        writer.writerows(rows)


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
    columns = {name: [row.get(name) for row in rows] for name in fieldnames}
    orc.write_table(pa.table(columns), path)


def write_parquet(path: str, rows: list[dict]) -> None:
    """Parquet writer using pyarrow"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Explicit columns, not pa.Table.from_pylist: that infers the schema from the first
    # row only, so a column that first appears in a later row would be silently dropped.
    # Missing values become None, so every row contributes its full key set.
    fieldnames = _column_union(rows)

    columns = {name: [row.get(name) for row in rows] for name in fieldnames}
    pq.write_table(pa.table(columns), path)


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
            "Install it with: pip install 'omniload[iterable]'"
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
