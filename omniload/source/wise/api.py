from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError


class WiseSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed = urlparse(uri)
        params = parse_qs(parsed.query)
        api_key = params.get("api_key")

        if not api_key:
            raise MissingValueError("api_key", "Wise")

        if table not in ["profiles", "transfers", "balances"]:
            raise ValueError(
                f"Resource '{table}' is not supported for Wise source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
            )

        start_date = kwargs.get("interval_start")
        if start_date:
            start_date = ensure_pendulum_datetime_utc(start_date).in_timezone("UTC")
        else:
            start_date = pendulum.datetime(2020, 1, 1).in_timezone("UTC")

        end_date = kwargs.get("interval_end")
        if end_date:
            end_date = ensure_pendulum_datetime_utc(end_date).in_timezone("UTC")
        else:
            end_date = None

        from omniload.source.wise.adapter import wise_source

        return wise_source(
            api_key=api_key[0],
            start_date=start_date,
            end_date=end_date,
        ).with_resources(table)
