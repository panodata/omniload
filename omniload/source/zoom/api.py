from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class ZoomSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Zoom takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed = urlparse(uri)
        params = parse_qs(parsed.query)
        client_id = params.get("client_id")
        client_secret = params.get("client_secret")
        account_id = params.get("account_id")

        if not (client_id and client_secret and account_id):
            raise MissingValueError(
                "client_id/client_secret/account_id",
                "Zoom",
            )

        start_date = kwargs.get("interval_start")
        if start_date is not None:
            start_date = ensure_pendulum_datetime_utc(start_date)
        else:
            start_date = pendulum.datetime(2020, 1, 26).in_tz("UTC")

        end_date = kwargs.get("interval_end")
        if end_date is not None:
            end_date = end_date = ensure_pendulum_datetime_utc(end_date).in_tz("UTC")

        from omniload.source.zoom.adapter import zoom_source

        if table not in {"meetings", "users", "participants"}:
            raise UnsupportedResourceError(table, "Zoom")

        return zoom_source(
            client_id=client_id[0],
            client_secret=client_secret[0],
            account_id=account_id[0],
            start_date=start_date,
            end_date=end_date,
        ).with_resources(table)
