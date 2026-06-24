from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc


class RedditAdsSource:
    ENTITY_TABLES = [
        "accounts",
        "campaigns",
        "ad_groups",
        "ads",
        "posts",
        "custom_audiences",
        "saved_audiences",
        "pixels",
        "funding_instruments",
    ]

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Reddit Ads takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        source_fields = parse_qs(parsed_uri.query)

        access_token = source_fields.get("access_token")
        if not access_token:
            raise ValueError("access_token is required to connect to Reddit Ads")

        account_ids = source_fields.get("account_ids")
        if not account_ids:
            raise ValueError("account_ids is required to connect to Reddit Ads")
        account_ids = account_ids[0].replace(" ", "").split(",")

        if table.startswith("custom:"):
            from omniload.source.reddit_ads.helpers import parse_custom_table

            level, breakdowns, metrics = parse_custom_table(table)

            interval_start = kwargs.get("interval_start")
            interval_end = kwargs.get("interval_end")
            start_date = (
                ensure_pendulum_datetime_utc(interval_start).date()
                if interval_start
                else pendulum.date(2020, 1, 1)
            )
            end_date = (
                ensure_pendulum_datetime_utc(interval_end).date()
                if interval_end
                else None
            )

            from omniload.source.reddit_ads.adapter import reddit_ads_analytics_source

            return reddit_ads_analytics_source(
                access_token=access_token[0],
                account_ids=account_ids,
                level=level,
                breakdowns=breakdowns,
                metrics=metrics,
                start_date=start_date,
                end_date=end_date,
            ).with_resources("custom_reports")

        if table not in self.ENTITY_TABLES:
            raise ValueError(
                f"Unsupported table '{table}' for Reddit Ads. Valid tables: {', '.join(self.ENTITY_TABLES)} or custom:<level>,<breakdowns>:<metrics>"
            )

        from omniload.source.reddit_ads.adapter import reddit_ads_source

        return reddit_ads_source(
            access_token=access_token[0],
            account_ids=account_ids,
        ).with_resources(table)
