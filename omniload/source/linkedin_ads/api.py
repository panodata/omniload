from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc


class LinkedInAdsSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "LinkedIn Ads takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        source_fields = parse_qs(parsed_uri.query)

        access_token = source_fields.get("access_token")
        if not access_token:
            raise ValueError("access_token is required to connect to LinkedIn Ads")

        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")
        start_datetime = (
            ensure_pendulum_datetime_utc(interval_start)
            if interval_start
            else pendulum.datetime(2018, 1, 1)
        )
        end_datetime = (
            ensure_pendulum_datetime_utc(interval_end) if interval_end else None
        )

        if table.startswith("custom:"):
            account_ids = source_fields.get("account_ids")
            if not account_ids:
                raise ValueError("account_ids is required to connect to LinkedIn Ads")
            account_ids = account_ids[0].replace(" ", "").split(",")

            fields = table.split(":")
            if len(fields) != 3:
                raise ValueError(
                    "Invalid table format. Expected format: custom:<dimensions>:<metrics>"
                )

            dimensions = fields[1].replace(" ", "").split(",")
            dimensions = [item for item in dimensions if item.strip()]
            valid_entity_dimensions = {
                "campaign",
                "creative",
                "account",
                "member_job_title",
                "member_seniority",
                "member_industry",
                "member_company_size",
                "member_company",
            }
            if not valid_entity_dimensions.intersection(dimensions):
                raise ValueError(
                    "A valid dimension is required to connect to LinkedIn Ads. "
                    "Please provide one of: campaign, creative, account, "
                    "member_job_title, member_seniority, member_industry, "
                    "member_company_size, member_company."
                )
            if "date" not in dimensions and "month" not in dimensions:
                raise ValueError(
                    "'date' or 'month' is required to connect to LinkedIn Ads, please provide at least one of these dimensions."
                )

            from omniload.source.linkedin_ads.adapter import (
                linked_in_ads_analytics_source,
            )
            from omniload.source.linkedin_ads.model import (
                Dimension,
                TimeGranularity,
            )

            if "date" in dimensions:
                time_granularity = TimeGranularity.daily
                dimensions.remove("date")
            else:
                time_granularity = TimeGranularity.monthly
                dimensions.remove("month")

            dimension = Dimension[dimensions[0]]

            metrics = fields[2].replace(" ", "").split(",")
            metrics = [item for item in metrics if item.strip()]
            if "dateRange" not in metrics:
                metrics.append("dateRange")
            if "pivotValues" not in metrics:
                metrics.append("pivotValues")

            return linked_in_ads_analytics_source(
                start_date=start_datetime.date(),
                end_date=(end_datetime.date() if end_datetime else None),
                access_token=access_token[0],
                account_ids=account_ids,
                dimension=dimension,
                metrics=metrics,
                time_granularity=time_granularity,
            ).with_resources("custom_reports")

        from omniload.source.linkedin_ads.adapter import linked_in_ads_source

        return linked_in_ads_source(
            access_token=access_token[0],
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        ).with_resources(table)
