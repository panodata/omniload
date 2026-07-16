from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc


class MixpanelSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Mixpanel takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed = urlparse(uri)
        params = parse_qs(parsed.query)
        username = params.get("username")
        password = params.get("password")
        api_secret = params.get("api_secret")
        project_id = params.get("project_id")
        server = params.get("server", ["eu"])

        has_service_account = username and password
        has_api_secret = api_secret

        if not has_service_account and not has_api_secret:
            raise ValueError(
                "Either (username, password) for Service Account auth or api_secret for Project Secret auth is required to connect to Mixpanel"
            )
        if has_service_account and not project_id:
            raise ValueError(
                "project_id is required to connect to Mixpanel when using service account authentication"
            )

        if table not in ["events", "profiles"]:
            raise ValueError(
                f"Resource '{table}' is not supported for Mixpanel source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
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
            end_date = pendulum.now().in_timezone("UTC")

        from omniload.source.mixpanel.adapter import mixpanel_source

        if has_service_account:
            auth_username = username[0]  # ty: ignore[not-subscriptable]
            auth_password = password[0]  # ty: ignore[not-subscriptable]
        else:
            auth_username = api_secret[0]  # ty: ignore[not-subscriptable]
            auth_password = ""

        return mixpanel_source(
            username=auth_username,
            password=auth_password,
            project_id=project_id[0] if project_id else None,
            start_date=start_date,
            end_date=end_date,
            server=server[0],
        ).with_resources(table)
