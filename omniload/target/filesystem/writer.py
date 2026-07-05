def write_csv(path: str, rows: list[dict]) -> None:
    """CSV writer using csv.DictWriter"""
    import csv

    # Union of keys in first-seen order: dlt omits null keys per row, so a later row can
    # carry a column the first row lacked. First-seen order preserves the source column
    # order (rather than sorting), which is what an export is expected to look like.
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, restval="")
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: str, rows: list[dict]) -> None:
    """JSONL writer using json.dumps"""
    # dlt's json handles datetime/Decimal/etc. that dlt may have produced; stdlib json
    # would choke on them.
    from dlt.common import json

    with open(path, "w") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def write_parquet(path: str, rows: list[dict]) -> None:
    """Parquet writer using pyarrow"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Union of keys, same reasoning as write_csv: dlt omits null keys per row, and
    # pa.Table.from_pylist infers the schema from the first row only, so a column that
    # first appears in a later row would be silently dropped. Build explicit columns
    # (missing values become None) so every row contributes its full key set.
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    columns = {name: [row.get(name) for row in rows] for name in fieldnames}
    pq.write_table(pa.table(columns), path)
