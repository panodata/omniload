import dlt
from yarl import URL

from omniload.target.model import GenericSqlDestination


class DeltaLakeDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        kwargs.pop("dest_table", None)
        kwargs.pop("staging_bucket", None)
        uri = uri.replace("+delta://", "://")
        # TODO: Review: Why not prune query parameters from URL when conveyed via `credentials`?
        url = URL(uri)
        creds = url.query
        return dlt.destinations.filesystem(bucket_url=uri, credentials=creds, **kwargs)

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        params = super().dlt_run_params(uri, table, **kwargs)
        params["table_format"] = "delta"
        return params
