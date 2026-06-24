from urllib.parse import parse_qs, urlparse


class FacebookAdsSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        # facebook_ads://?access_token=abcd&account_id=1234
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Facebook Ads takes care of incrementality on its own, you should not provide incremental_key"
            )

        access_token = None
        account_id = None
        source_field = urlparse(uri)
        source_params = parse_qs(source_field.query)
        access_token = source_params.get("access_token")
        account_id = source_params.get("account_id")

        if not access_token:
            raise ValueError("access_token is required to connect to Facebook Ads.")

        from omniload.source.facebook_ads.adapter import (
            facebook_ads_source,
            facebook_insights_source,
            facebook_insights_with_account_ids_source,
        )

        insights_max_wait_to_finish_seconds = source_params.get(
            "insights_max_wait_to_finish_seconds", [60 * 60 * 4]
        )
        insights_max_wait_to_start_seconds = source_params.get(
            "insights_max_wait_to_start_seconds", [60 * 30]
        )
        insights_max_async_sleep_seconds = source_params.get(
            "insights_max_async_sleep_seconds", [20]
        )

        endpoint = None
        table_account_ids = None

        if table in ["campaigns", "ad_sets", "ad_creatives", "ads", "leads"]:
            endpoint = table
        elif ":" in table and table.split(":")[0] in [
            "campaigns",
            "ad_sets",
            "ad_creatives",
            "ads",
            "leads",
        ]:
            parts = table.split(":")
            endpoint = parts[0]
            table_account_ids = [a.strip() for a in parts[1].split(",") if a.strip()]
        elif table == "facebook_insights":
            if not account_id:
                raise ValueError(
                    "account_id is required for facebook_insights. Provide it in the URI (?account_id=xxx) or use facebook_insights_with_account_ids:account_id1,account_id2"
                )
            return facebook_insights_source(
                access_token=access_token[0],
                account_id=account_id[0],
                start_date=kwargs.get("interval_start"),
                end_date=kwargs.get("interval_end"),
                insights_max_wait_to_finish_seconds=int(
                    insights_max_wait_to_finish_seconds[0]
                ),
                insights_max_wait_to_start_seconds=int(
                    insights_max_wait_to_start_seconds[0]
                ),
                insights_max_async_sleep_seconds=int(
                    insights_max_async_sleep_seconds[0]
                ),
            ).with_resources("facebook_insights")
        elif table.startswith("facebook_insights_with_account_ids:"):
            parts = table.split(":")
            if len(parts) < 2:
                raise ValueError(
                    "Invalid facebook_insights_with_account_ids format. Expected: facebook_insights_with_account_ids:account_id1,account_id2"
                )

            multi_account_ids = [a.strip() for a in parts[1].split(",") if a.strip()]
            if not multi_account_ids:
                raise ValueError(
                    "At least one account_id must be provided in format: facebook_insights_with_account_ids:account_id1,account_id2"
                )

            from omniload.source.facebook_ads.helpers import (
                parse_insights_table_to_source_kwargs,
            )

            source_kwargs = {
                "access_token": access_token[0],
                "account_ids": multi_account_ids,
                "start_date": kwargs.get("interval_start"),
                "end_date": kwargs.get("interval_end"),
                "insights_max_wait_to_finish_seconds": int(
                    insights_max_wait_to_finish_seconds[0]
                ),
                "insights_max_wait_to_start_seconds": int(
                    insights_max_wait_to_start_seconds[0]
                ),
                "insights_max_async_sleep_seconds": int(
                    insights_max_async_sleep_seconds[0]
                ),
            }

            if len(parts) > 2:
                remaining_table = ":".join(["facebook_insights"] + parts[2:])
                source_kwargs.update(
                    parse_insights_table_to_source_kwargs(remaining_table)
                )

            return facebook_insights_with_account_ids_source(
                **source_kwargs  # ty: ignore[invalid-argument-type]
            ).with_resources("facebook_insights")
        elif table.startswith("facebook_insights:"):
            # Parse custom breakdowns and metrics from table name
            # Supported formats:
            # facebook_insights:breakdown_type
            # facebook_insights:breakdown_type:metric1,metric2...
            parts = table.split(":")

            if len(parts) < 2 or len(parts) > 3:
                raise ValueError(
                    "Invalid facebook_insights format. Expected: facebook_insights:breakdown_type or facebook_insights:breakdown_type:metric1,metric2..."
                )

            breakdown_type = parts[1].strip()
            if not breakdown_type:
                raise ValueError(
                    "Breakdown type must be provided in format: facebook_insights:breakdown_type"
                )

            if not account_id:
                raise ValueError(
                    "account_id is required for facebook_insights. Provide it in the URI (?account_id=xxx) or use facebook_insights_with_account_ids:account_id1,account_id2"
                )

            # Validate breakdown type against available options from settings

            from omniload.source.facebook_ads.helpers import (
                parse_insights_table_to_source_kwargs,
            )

            source_kwargs = {
                "access_token": access_token[0],
                "account_id": account_id[0],
                "start_date": kwargs.get("interval_start"),
                "end_date": kwargs.get("interval_end"),
            }

            source_kwargs.update(parse_insights_table_to_source_kwargs(table))
            return facebook_insights_source(
                **source_kwargs  # ty: ignore[invalid-argument-type]
            ).with_resources("facebook_insights")
        else:
            raise ValueError(
                f"Resource '{table}' is not supported for Facebook Ads source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
            )

        final_account_ids = table_account_ids if table_account_ids else account_id

        if not final_account_ids:
            raise ValueError(
                "account_id is required. Provide it in the URI (?account_id=xxx) or in the table name (campaigns:xxx)"
            )

        return facebook_ads_source(
            access_token=access_token[0],
            account_id=final_account_ids
            if len(final_account_ids) > 1
            else final_account_ids[0],
            interval_start=kwargs.get("interval_start"),
            interval_end=kwargs.get("interval_end"),
        ).with_resources(endpoint)
