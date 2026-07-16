from urllib.parse import parse_qs, urlparse

import dlt

from omniload.error import MissingValueError, UnsupportedResourceError


class CursorSource:
    resources = [
        "team_members",
        "daily_usage_data",
        "team_spend",
        "filtered_usage_events",
    ]

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        # cursor://?api_key=<api_key>
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        api_key = params.get("api_key")

        if not api_key:
            raise MissingValueError("api_key", "Cursor")

        if table not in self.resources:
            raise UnsupportedResourceError(table, "Cursor")

        from omniload.source.cursor.adapter import cursor_source

        dlt.secrets["sources.cursor.api_key"] = api_key[0]

        # Handle interval_start and interval_end for daily_usage_data and filtered_usage_events (optional)
        if table in ["daily_usage_data", "filtered_usage_events"]:
            interval_start = kwargs.get("interval_start")
            interval_end = kwargs.get("interval_end")

            # Both are optional, but if one is provided, both should be provided
            if interval_start is not None and interval_end is not None:
                # Convert datetime to epoch milliseconds
                start_ms = int(interval_start.timestamp() * 1000)
                end_ms = int(interval_end.timestamp() * 1000)

                dlt.config["sources.cursor.start_date"] = start_ms
                dlt.config["sources.cursor.end_date"] = end_ms

        src = cursor_source()
        return src.with_resources(table)
