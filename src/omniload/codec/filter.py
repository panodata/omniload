def cast_set_to_list(row):
    # this handles just the sqlalchemy backend for now
    if isinstance(row, dict):
        for key in row.keys():
            if isinstance(row[key], set):
                row[key] = list(row[key])
    return row


def cast_spanner_types(row):
    if not isinstance(row, dict):
        return row

    from google.cloud.spanner_v1.data_types import JsonObject

    for key in row.keys():
        if isinstance(row[key], JsonObject):
            from dlt.common import json

            row[key] = json.loads(row[key].serialize())
    return row


def handle_mysql_empty_dates(row):
    # MySQL returns empty dates as 0000-00-00, which is not a valid date.
    # We handle them here.
    if not isinstance(row, dict):
        return row

    for key in row.keys():
        if not isinstance(row[key], str):
            continue

        if row[key] == "0000-00-00":
            from datetime import date

            row[key] = date(1970, 1, 1)

        elif row[key] == "0000-00-00 00:00:00":
            from datetime import datetime

            row[key] = datetime(1970, 1, 1, 0, 0, 0)
    return row


def table_adapter_exclude_columns(cols: list[str]):
    from dlt.common.libs.sql_alchemy import Table

    def excluder(table: Table):
        cols_to_remove = [col for col in table._columns if col.name in cols]  # ty: ignore[missing-attribute, unused-ignore-comment, unused-ignore-comment]
        for col in cols_to_remove:
            table._columns.remove(col)  # ty: ignore[missing-attribute, unused-ignore-comment, unused-ignore-comment]

    return excluder


def create_masking_filter(mask_configs: list[str]):
    from omniload.codec.masking import create_masking_mapper

    if not mask_configs:
        return lambda x: x

    return create_masking_mapper(mask_configs)


_NS_PER_UNIT = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}
_NS_PER_SECOND = 1_000_000_000
_NS_PER_DAY = 86_400 * _NS_PER_SECOND


def format_interval(total_ns: int) -> str:
    """Format a signed nanosecond count as interval text.

    The form follows PostgreSQL's own output, `[-]D days [-]HH:MM:SS[.fffffffff]`,
    with the sign carried on each field, so PostgreSQL and DuckDB both cast it
    back to an `INTERVAL`. The fraction keeps nanoseconds and drops trailing
    zeros, and a whole number of days has no clock part.
    """
    sign = "-" if total_ns < 0 else ""
    days, rest = divmod(abs(total_ns), _NS_PER_DAY)
    seconds, fraction = divmod(rest, _NS_PER_SECOND)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    clock = f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    if fraction:
        clock += "." + f"{fraction:09d}".rstrip("0")
    if not days:
        return clock
    day_part = f"{sign}{days} {'day' if days == 1 and not sign else 'days'}"
    return f"{day_part} {clock}" if rest else day_part


def _timedelta_ns(value) -> int:
    from datetime import timedelta

    # A pandas `Timedelta` carries nanoseconds past the microseconds that
    # `datetime.timedelta` holds; floor division keeps them consistent.
    return (value // timedelta(microseconds=1)) * 1_000 + getattr(
        value, "nanoseconds", 0
    )


def _durations_as_text(column, unit: str):
    import pyarrow as pa

    factor = _NS_PER_UNIT[unit]

    def convert(array):
        counts = array.cast(pa.int64()).to_pylist()
        return pa.array(
            [None if n is None else format_interval(n * factor) for n in counts],
            type=pa.string(),
        )

    if isinstance(column, pa.ChunkedArray):
        return pa.chunked_array(
            [convert(chunk) for chunk in column.chunks], type=pa.string()
        )
    return convert(column)


def cast_timedelta(item):
    """Turn `timedelta` values and Arrow `duration` columns into interval text.

    dlt has no interval type: it cannot serialize a `timedelta` row value, and it
    rejects an Arrow `duration` column. Covers `datetime.timedelta` and its
    subclasses, pandas `Timedelta` included. Values nested in another value are
    left alone.
    """
    from datetime import timedelta

    if isinstance(item, dict):
        for key, value in item.items():
            if isinstance(value, timedelta):
                item[key] = format_interval(_timedelta_ns(value))
        return item

    import pyarrow as pa

    if not isinstance(item, (pa.Table, pa.RecordBatch)):
        return item
    schema = item.schema
    if not any(pa.types.is_duration(field.type) for field in schema):
        return item

    columns = []
    fields = []
    for index, field in enumerate(schema):
        column = item.column(index)
        if pa.types.is_duration(field.type):
            column = _durations_as_text(column, field.type.unit)
            field = field.with_type(pa.string())
        columns.append(column)
        fields.append(field)
    return type(item).from_arrays(
        columns, schema=pa.schema(fields, metadata=schema.metadata)
    )
