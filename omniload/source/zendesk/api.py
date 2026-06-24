from urllib.parse import urlparse


class ZendeskSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Zendesk takes care of incrementality on its own, you should not provide incremental_key"
            )

        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")
        start_date = (
            interval_start.strftime("%Y-%m-%d") if interval_start else "2000-01-01"
        )
        end_date = interval_end.strftime("%Y-%m-%d") if interval_end else None

        source_fields = urlparse(uri)
        subdomain = source_fields.hostname
        if not subdomain:
            raise ValueError("Subdomain is required to connect with Zendesk")

        from omniload.source.zendesk.adapter import (
            zendesk_chat,
            zendesk_support,
            zendesk_talk,
        )
        from omniload.source.zendesk.helpers.credentials import (
            ZendeskCredentialsOAuth,
            ZendeskCredentialsToken,
        )

        if not source_fields.username and source_fields.password:
            oauth_token = source_fields.password
            if not oauth_token:
                raise ValueError(
                    "oauth_token in the URI is required to connect to Zendesk"
                )
            credentials = ZendeskCredentialsOAuth(
                subdomain=subdomain, oauth_token=oauth_token
            )
        elif source_fields.username and source_fields.password:
            email = source_fields.username
            api_token = source_fields.password
            if not email or not api_token:
                raise ValueError(
                    "Both email and token must be provided to connect to Zendesk"
                )
            credentials = ZendeskCredentialsToken(
                subdomain=subdomain, email=email, token=api_token
            )
        else:
            raise ValueError("Invalid URI format")

        if table in [
            "ticket_metrics",
            "users",
            "ticket_metric_events",
            "ticket_forms",
            "tickets",
            "targets",
            "activities",
            "brands",
            "groups",
            "organizations",
            "sla_policies",
            "automations",
        ]:
            return zendesk_support(
                credentials=credentials, start_date=start_date, end_date=end_date
            ).with_resources(table)
        elif table in [
            "greetings",
            "settings",
            "addresses",
            "legs_incremental",
            "calls",
            "phone_numbers",
            "lines",
            "agents_activity",
        ]:
            return zendesk_talk(
                credentials=credentials, start_date=start_date, end_date=end_date
            ).with_resources(table)
        elif table in ["chats"]:
            return zendesk_chat(
                credentials=credentials, start_date=start_date, end_date=end_date
            ).with_resources(table)
        else:
            raise ValueError(
                f"Resource '{table}' is not supported for Zendesk source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
            )
