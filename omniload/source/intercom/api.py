from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class IntercomSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        # intercom://?access_token=<token>&region=<us|eu|au>
        # OR intercom://?oauth_token=<token>&region=<us|eu|au>
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        # Check for authentication
        access_token = params.get("access_token")
        oauth_token = params.get("oauth_token")
        region = params.get("region", ["us"])[0]

        if not access_token and not oauth_token:
            raise MissingValueError("access_token or oauth_token", "Intercom")

        # Validate table/resource
        supported_tables = [
            "contacts",
            "companies",
            "conversations",
            "tickets",
            "tags",
            "segments",
            "teams",
            "admins",
            "articles",
            "data_attributes",
        ]

        if table not in supported_tables:
            raise UnsupportedResourceError(table, "Intercom")

        # Get date parameters
        start_date = kwargs.get("interval_start")
        if start_date:
            start_date = ensure_pendulum_datetime_utc(start_date)
        else:
            start_date = pendulum.datetime(2020, 1, 1)

        end_date = kwargs.get("interval_end")
        if end_date:
            end_date = ensure_pendulum_datetime_utc(end_date)

        # Import and initialize the source
        from omniload.source.intercom.adapter import (
            IntercomCredentialsAccessToken,
            IntercomCredentialsOAuth,
            TIntercomCredentials,
            intercom_source,
        )

        credentials: TIntercomCredentials
        if access_token:
            credentials = IntercomCredentialsAccessToken(
                access_token=access_token[0], region=region
            )
        else:
            if not oauth_token:
                raise MissingValueError("oauth_token", "Intercom")
            credentials = IntercomCredentialsOAuth(
                oauth_token=oauth_token[0], region=region
            )

        return intercom_source(
            credentials=credentials,
            start_date=start_date,
            end_date=end_date,
        ).with_resources(table)
