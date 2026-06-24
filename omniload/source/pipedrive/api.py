from typing import cast
from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class PipedriveSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Pipedrive takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)
        api_key = params.get("api_token")
        if api_key is None:
            raise MissingValueError("api_token", "Pipedrive")

        start_dt: pendulum.DateTime
        start_date = kwargs.get("interval_start")
        if start_date is not None:
            start_dt = ensure_pendulum_datetime_utc(start_date)
        else:
            start_dt = cast(pendulum.DateTime, pendulum.parse("2000-01-01"))

        if table not in [
            "users",
            "activities",
            "persons",
            "organizations",
            "products",
            "stages",
            "deals",
        ]:
            raise UnsupportedResourceError(table, "Pipedrive")

        from omniload.source.pipedrive.adapter import pipedrive_source

        return pipedrive_source(
            pipedrive_api_key=str(api_key), since_timestamp=start_dt
        ).with_resources(table)
