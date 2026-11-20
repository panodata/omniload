from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy import types as sa
from sqlalchemy.dialects import mysql


def type_adapter_callback(sql_type):
    if isinstance(sql_type, mysql.SET):
        return sa.JSON
    return sql_type


def chained_query_adapter_callback(query_adapters):
    """
    This function is used to chain multiple query adapters together,.
    This gives us the flexibility to introduce various adapters based on the given command parameters.
    """

    # dlt calls a query adapter with the incremental and the engine when its
    # signature takes them, so accept both and hand them on to every adapter.
    def callback(query, table, incremental=None, engine=None):
        for adapter in query_adapters:
            query = adapter(query, table, incremental, engine)

        return query

    return callback


def limit_callback(sql_limit: int, incremental_key: Optional[str] = None):
    def callback(query, table, incremental=None, engine=None):
        query = query.limit(sql_limit)
        if incremental_key:
            query = query.order_by(incremental_key)
        return query

    return callback


def custom_query_variable_subsitution(query_value: str, kwargs: dict):
    def callback(query, table, incremental=None, engine=None):
        # Bind only the placeholders the query names: text() refuses a value for a
        # parameter it does not define. With an incremental key, a resumed run
        # binds the stored cursor; a first run binds --interval-start, as dlt
        # seeds the incremental from it.
        params = {}
        if ":interval_start" in query_value:
            start = incremental.last_value if incremental is not None else None
            if start is None:
                start = kwargs.get("interval_start")
            params["interval_start"] = datetime.min if start is None else start
        if ":interval_end" in query_value:
            end = incremental.end_value if incremental is not None else None
            if end is None:
                end = kwargs.get("interval_end")
            params["interval_end"] = datetime.max if end is None else end

        return text(query_value).bindparams(**params)

    return callback
