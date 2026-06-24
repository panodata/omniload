from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class ClickupSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "ClickUp takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)
        api_token = params.get("api_token")

        if api_token is None:
            raise MissingValueError("api_token", "ClickUp")

        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")
        start_date = (
            ensure_pendulum_datetime_utc(interval_start).in_timezone("UTC")
            if interval_start
            else pendulum.datetime(2020, 1, 1, tz="UTC")
        )
        end_date = (
            ensure_pendulum_datetime_utc(interval_end).in_timezone("UTC")
            if interval_end
            else None
        )

        from omniload.source.clickup.adapter import clickup_source

        if table not in {"user", "teams", "lists", "tasks", "spaces"}:
            raise UnsupportedResourceError(table, "ClickUp")

        return clickup_source(
            api_token=api_token[0], start_date=start_date, end_date=end_date
        ).with_resources(table)
