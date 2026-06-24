from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse

from omniload.error import MissingValueError, UnsupportedResourceError


class AppLovinSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key") is not None:
            raise ValueError(
                "Applovin takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        api_key = params.get("api_key", None)
        if api_key is None:
            raise MissingValueError("api_key", "AppLovin")

        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")

        now = datetime.now()
        start_date = (
            interval_start if interval_start is not None else now - timedelta(days=1)
        )
        end_date = interval_end

        custom_report = None
        if table.startswith("custom:"):
            custom_report = table
            table = "custom_report"

        from omniload.source.applovin.adapter import applovin_source

        src = applovin_source(
            api_key[0],
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d") if end_date else None,
            custom_report,
        )

        if table not in src.resources:
            raise UnsupportedResourceError(table, "AppLovin")

        return src.with_resources(table)
