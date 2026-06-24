from urllib.parse import parse_qs, urlparse

from omniload.error import MissingValueError, UnsupportedResourceError


class CustomerIoSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        api_key = params.get("api_key")
        if not api_key:
            raise MissingValueError("api_key", "Customer.io")

        region = params.get("region", ["us"])[0]
        if region.lower() not in ["us", "eu"]:
            raise ValueError(
                f"Invalid region '{region}' for Customer.io. Must be one of: us, eu"
            )

        # Handle metrics tables with period format (e.g., broadcast_metrics:days)
        metrics_sources = {
            "broadcast_metrics": ("customer_io_broadcast_metrics_source", False),
            "broadcast_action_metrics": (
                "customer_io_broadcast_action_metrics_source",
                False,
            ),
            "campaign_metrics": ("customer_io_campaign_metrics_source", True),
            "campaign_action_metrics": (
                "customer_io_campaign_action_metrics_source",
                True,
            ),
            "newsletter_metrics": ("customer_io_newsletter_metrics_source", False),
        }

        for prefix, (source_name, needs_dates) in metrics_sources.items():
            if table.startswith(f"{prefix}:"):
                parts = table.split(":")
                period = parts[1]

                if period not in ["hours", "days", "weeks", "months"]:
                    raise ValueError(
                        f"Invalid period '{period}' for {prefix}. Must be one of: hours, days, weeks, months"
                    )

                from omniload.source import customer_io

                source_func = getattr(customer_io, source_name)

                source_kwargs: dict = {
                    "api_key": api_key[0],
                    "region": region,
                    "period": period,
                }
                if needs_dates:
                    source_kwargs["start_date"] = kwargs.get("interval_start")
                    source_kwargs["end_date"] = kwargs.get("interval_end")

                return source_func(**source_kwargs)

        if table not in [
            "activities",
            "broadcasts",
            "broadcast_actions",
            "broadcast_messages",
            "campaigns",
            "campaign_actions",
            "campaign_messages",
            "collections",
            "exports",
            "info_ip_addresses",
            "messages",
            "newsletters",
            "newsletter_test_groups",
            "reporting_webhooks",
            "segments",
            "sender_identities",
            "transactional_messages",
            "workspaces",
            "customers",
            "customer_attributes",
            "customer_messages",
            "customer_activities",
            "customer_relationships",
            "object_types",
            "objects",
            "subscription_topics",
        ]:
            raise UnsupportedResourceError(table, "Customer.io")

        start_date = kwargs.get("interval_start")
        end_date = kwargs.get("interval_end")

        from omniload.source.customer_io.adapter import customer_io_source

        return customer_io_source(
            api_key=api_key[0],
            region=region,
            start_date=start_date,
            end_date=end_date,
        ).with_resources(table)
