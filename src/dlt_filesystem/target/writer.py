"""Format writers for the local ``file://`` destination.

Every writer takes ``(path, rows)`` and emits one file. Output is UTF-8 whatever the
process locale is, because the readers decode as UTF-8 unconditionally (``json.loadb``
rejects anything else, and Polars defaults to it), so a locale-encoded export would not
read back on the machine that wrote it.
"""


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
    if rows and not fieldnames:
        raise ValueError("ORC output requires at least one column for nonempty rows")
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
