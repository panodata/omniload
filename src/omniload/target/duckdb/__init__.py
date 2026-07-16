import dlt

from omniload.target.model import GenericSqlDestination


class DuckDBDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        kwargs.pop("dest_table", None)
        kwargs.pop("staging_bucket", None)
        return dlt.destinations.duckdb(uri, **kwargs)
