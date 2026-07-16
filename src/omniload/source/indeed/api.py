from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class IndeedSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        client_id = params.get("client_id")
        if client_id is None:
            raise MissingValueError("client_id", "Indeed")

        client_secret = params.get("client_secret")
        if client_secret is None:
            raise MissingValueError("client_secret", "Indeed")

        employer_id = params.get("employer_id")
        if employer_id is None:
            raise MissingValueError("employer_id", "Indeed")

        if table not in [
            "campaigns",
            "campaign_details",
            "campaign_budget",
            "campaign_jobs",
            "campaign_properties",
            "campaign_stats",
            "account",
            "traffic_stats",
        ]:
            raise UnsupportedResourceError(table, "Indeed")

        start_date = kwargs.get("interval_start")
        if start_date is not None:
            start_date = ensure_pendulum_datetime_utc(start_date)
        else:
            start_date = pendulum.now("UTC").subtract(days=365)

        end_date = kwargs.get("interval_end")
        if end_date is not None:
            end_date = ensure_pendulum_datetime_utc(end_date).in_tz("UTC")

        from omniload.source.indeed.adapter import indeed_source

        return indeed_source(
            client_id=client_id[0],
            client_secret=client_secret[0],
            employer_id=employer_id[0],
            start_date=start_date,
            end_date=end_date,
        ).with_resources(table)
