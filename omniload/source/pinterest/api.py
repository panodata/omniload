from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class PinterestSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Pinterest takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed = urlparse(uri)
        params = parse_qs(parsed.query)
        access_token = params.get("access_token")

        if not access_token:
            raise MissingValueError("access_token", "Pinterest")

        start_date = kwargs.get("interval_start")
        if start_date is not None:
            start_date = ensure_pendulum_datetime_utc(start_date)
        else:
            start_date = pendulum.datetime(2020, 1, 1).in_tz("UTC")

        end_date = kwargs.get("interval_end")
        if end_date is not None:
            end_date = end_date = ensure_pendulum_datetime_utc(end_date).in_tz("UTC")

        from omniload.source.pinterest.adapter import pinterest_source

        if table not in {"pins", "boards"}:
            raise UnsupportedResourceError(table, "Pinterest")

        return pinterest_source(
            access_token=access_token[0],
            start_date=start_date,
            end_date=end_date,
        ).with_resources(table)
