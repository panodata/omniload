from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class AnthropicSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        # anthropic://?api_key=<admin_api_key>
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        api_key = params.get("api_key")
        if api_key is None:
            raise MissingValueError("api_key", "Anthropic")

        if table not in [
            "claude_code_usage",
            "usage_report",
            "cost_report",
            "organization",
            "workspaces",
            "api_keys",
            "invites",
            "users",
            "workspace_members",
        ]:
            raise UnsupportedResourceError(table, "Anthropic")

        # Get start and end dates from kwargs
        start_date = kwargs.get("interval_start")
        if start_date:
            start_date = ensure_pendulum_datetime_utc(start_date)
        else:
            # Default to 2023-01-01
            start_date = pendulum.datetime(2023, 1, 1)

        end_date = kwargs.get("interval_end")
        if end_date:
            end_date = ensure_pendulum_datetime_utc(end_date)
        else:
            end_date = None

        from omniload.source.anthropic.adapter import anthropic_source

        return anthropic_source(
            api_key=api_key[0],
            initial_start_date=start_date,
            end_date=end_date,
        ).with_resources(table)
