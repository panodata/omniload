from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class RevenueCatSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "RevenueCat takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        api_key = params.get("api_key")
        if api_key is None:
            raise MissingValueError("api_key", "RevenueCat")

        project_id = params.get("project_id")
        if project_id is None and table != "projects":
            raise MissingValueError("project_id", "RevenueCat")

        if table not in [
            "customers",
            "products",
            "entitlements",
            "offerings",
            "subscriptions",
            "purchases",
            "projects",
        ]:
            raise UnsupportedResourceError(table, "RevenueCat")

        start_date = kwargs.get("interval_start")
        if start_date is not None:
            start_date = ensure_pendulum_datetime_utc(start_date)
        else:
            start_date = pendulum.datetime(2020, 1, 1).in_tz("UTC")

        end_date = kwargs.get("interval_end")
        if end_date is not None:
            end_date = ensure_pendulum_datetime_utc(end_date).in_tz("UTC")

        from omniload.source.revenuecat.adapter import revenuecat_source

        return revenuecat_source(
            api_key=api_key[0],
            project_id=project_id[0] if project_id is not None else None,
        ).with_resources(table)
