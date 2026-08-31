from typing import Any
from urllib.parse import parse_qs, urlsplit, urlunsplit

import dlt

from dlt_filesystem.target.remote import blob_destination_options
from omniload.core.tablename import two_level
from omniload.target.model import GenericSqlDestination


class DeltaLakeDestination(GenericSqlDestination):
    """Destination adapter for writing to Delta Lake tables."""

    table_capability = two_level("Delta Lake")

    def dlt_dest(self, uri: str, **kwargs):
        kwargs.pop("dest_table", None)
        kwargs.pop("staging_bucket", None)
        parsed = urlsplit(uri.replace("+delta://", "://"))
        params = parse_qs(parsed.query)

        # Protocols dlt models no credential spec for (hdfs, lakefs) take no
        # options from the query; forwarding the raw strings to their fsspec
        # backend would hand a constructor `"false"` where it expects `False`.
        options: dict[str, Any] = blob_destination_options(parsed.scheme, params)

        # The query never travels in the bucket URL: dlt reads it as part of the
        # storage path, which on an object store makes the credential part of the
        # object key rather than of the request signature.
        return dlt.destinations.filesystem(
            bucket_url=urlunsplit(parsed._replace(query="")),
            **options,
            **kwargs,
        )

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        params = super().dlt_run_params(uri, table, **kwargs)
        params["table_format"] = "delta"
        return params
