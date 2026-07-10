from typing import Any, Sequence, Union

import sqlalchemy as sa


def dbquery(
    uri: str, query: str, fetch: bool = False, mappings: bool = False
) -> Union[Sequence[Union[sa.Row[Any], sa.RowMapping]], None]:
    """Query database using SQLAlchemy and optionally return results."""

    engine = sa.create_engine(uri, poolclass=sa.NullPool)
    response = None
    with engine.connect() as conn:
        res = conn.exec_driver_sql(query)
        if fetch:
            if mappings:
                response = res.mappings().fetchall()
            else:
                response = res.fetchall()
    engine.dispose()
    return response


def get_query_result(uri: str, query: str, fetch: bool = True, mappings: bool = False):
    """Query database using SQLAlchemy and return results."""
    return dbquery(uri, query, fetch=True, mappings=mappings)
