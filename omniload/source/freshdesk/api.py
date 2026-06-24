from typing import Optional
from urllib.parse import parse_qs, urlparse

from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class FreshdeskSource:
    # freshdesk://domain?api_key=<api_key>
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Freshdesk takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        domain = parsed_uri.netloc
        query = parsed_uri.query
        params = parse_qs(query)

        if not domain:
            raise MissingValueError("domain", "Freshdesk")

        if "." in domain:
            domain = domain.split(".")[0]

        api_key = params.get("api_key")
        if api_key is None:
            raise MissingValueError("api_key", "Freshdesk")

        start_date = kwargs.get("interval_start")
        if start_date is not None:
            start_date = ensure_pendulum_datetime_utc(start_date).in_tz("UTC")
        else:
            start_date = ensure_pendulum_datetime_utc("2022-01-01T00:00:00Z")

        end_date = kwargs.get("interval_end")
        if end_date is not None:
            end_date = ensure_pendulum_datetime_utc(end_date).in_tz("UTC")
        else:
            end_date = None

        custom_query: Optional[str] = None
        if ":" in table:
            table, custom_query = table.split(":", 1)

        if table not in [
            "agents",
            "companies",
            "contacts",
            "groups",
            "roles",
            "tickets",
        ]:
            raise UnsupportedResourceError(table, "Freshdesk")

        if custom_query and table != "tickets":
            raise ValueError(f"Custom query is not supported for {table}")

        from omniload.source.freshdesk.adapter import freshdesk_source

        return freshdesk_source(
            api_secret_key=api_key[0],
            domain=domain,
            start_date=start_date,
            end_date=end_date,
            query=custom_query,
        ).with_resources(table)
