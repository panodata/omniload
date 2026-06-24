from urllib.parse import urlparse

import dlt

from omniload.target.model import GenericSqlDestination


class MySqlDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        return dlt.destinations.sqlalchemy(credentials=uri)

    def dlt_run_params(self, uri: str, table: str, **kwargs):
        parsed = urlparse(uri)
        database = parsed.path.lstrip("/")
        if not database:
            raise ValueError("You need to specify a database")
        return {
            "dataset_name": database,
            "table_name": table,
        }
