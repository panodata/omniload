from urllib.parse import parse_qs, urlparse

import pendulum


class ApplovinMaxSource:
    # expected uri format: applovinmax://?api_key=<api_key>
    # expected table format: user_ad_revenue:app_id_1,app_id_2

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "AppLovin Max takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        api_key = params.get("api_key")
        if api_key is None:
            raise ValueError("api_key is required to connect to AppLovin Max API.")

        AVAILABLE_TABLES = ["user_ad_revenue"]

        table_fields = table.split(":")
        requested_table = table_fields[0]

        if len(table_fields) != 2:
            raise ValueError(
                "Invalid table format. Expected format is user_ad_revenue:app_id_1,app_id_2"
            )

        if requested_table not in AVAILABLE_TABLES:
            raise ValueError(
                f"Table name '{requested_table}' is not supported for AppLovin Max source yet."
                f"Only '{AVAILABLE_TABLES}' are currently supported. "
                "If you need additional tables, please create a GitHub issue at "
                "https://github.com/panodata/omniload"
            )

        applications = [
            i for i in table_fields[1].replace(" ", "").split(",") if i.strip()
        ]
        if len(applications) == 0:
            raise ValueError("At least one application id is required")

        if len(applications) != len(set(applications)):
            raise ValueError("Application ids must be unique.")

        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")

        now = pendulum.now("UTC")
        default_start = now.subtract(days=30).date()

        start_date = (
            interval_start.date() if interval_start is not None else default_start
        )

        end_date = interval_end.date() if interval_end is not None else None

        from omniload.source.applovin_max.adapter import applovin_max_source

        return applovin_max_source(
            start_date=start_date,
            end_date=end_date,
            api_key=api_key[0],
            applications=applications,
        ).with_resources(requested_table)
