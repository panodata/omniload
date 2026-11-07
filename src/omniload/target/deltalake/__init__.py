from typing import Any
from urllib.parse import parse_qs, urlsplit, urlunsplit

import dlt
from dlt_filesystem.target.remote import blob_destination_options
from dlt_filesystem.util.auth import query_option_names

from omniload.core.tablename import two_level
from omniload.error import ValidationError
from omniload.target.model import GenericSqlDestination


class DeltaLakeDestination(GenericSqlDestination):
    """Destination adapter for writing to Delta Lake tables."""

    table_capability = two_level("Delta Lake")

    def dlt_dest(self, uri: str, **kwargs):
        kwargs.pop("dest_table", None)
        kwargs.pop("staging_bucket", None)
        parsed = urlsplit(uri.replace("+delta://", "://"))
        params = parse_qs(parsed.query)
        # parse_qs drops blank values, so `access_key_id=` would otherwise read as
        # no query at all. The raw option names keep a blank key refused, and let
        # S3 tell a query naming no key from one naming a blank key.
        named_options = query_option_names(parsed.query)

        # The blob destinations shape their output paths with `layout`; a Delta
        # table has its own layout, so the option would be silently ignored.
        if "layout" in named_options:
            raise ValidationError(
                "The Delta Lake destination does not support the 'layout' URI "
                "option: a Delta table defines its own file layout. Remove "
                "'layout' from the destination URI."
            )
        named = any(key != "layout" for key in named_options)

        # Protocols dlt models no credential spec for (hdfs, lakefs) take no
        # options from the query; forwarding the raw strings to their fsspec
        # backend would hand a constructor `"false"` where it expects `False`.
        options: dict[str, Any] = blob_destination_options(
            parsed.scheme, params, named=named, named_options=named_options
        )

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
