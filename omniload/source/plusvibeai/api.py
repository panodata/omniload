from urllib.parse import parse_qs, urlparse

import dlt

from omniload.error import MissingValueError, UnsupportedResourceError


class PlusVibeAISource:
    resources = [
        "campaigns",
        "leads",
        "email_accounts",
        "emails",
        "blocklist",
        "webhooks",
        "tags",
    ]

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        # plusvibeai://?api_key=<key>&workspace_id=<id>
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        api_key = params.get("api_key")
        workspace_id = params.get("workspace_id")

        if not api_key:
            raise MissingValueError("api_key", "PlusVibeAI")

        if not workspace_id:
            raise MissingValueError("workspace_id", "PlusVibeAI")

        if table not in self.resources:
            raise UnsupportedResourceError(table, "PlusVibeAI")

        from omniload.source.plusvibeai.adapter import plusvibeai_source

        dlt.secrets["sources.plusvibeai.api_key"] = api_key[0]
        dlt.secrets["sources.plusvibeai.workspace_id"] = workspace_id[0]

        # Handle custom base URL if provided
        base_url = params.get("base_url", ["https://api.plusvibe.ai"])[0]
        dlt.secrets["sources.plusvibeai.base_url"] = base_url

        src = plusvibeai_source()
        return src.with_resources(table)
