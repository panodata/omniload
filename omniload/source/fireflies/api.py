from urllib.parse import parse_qs, urlparse

from omniload.error import MissingValueError, UnsupportedResourceError


class FirefliesSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Fireflies takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        source_fields = parse_qs(parsed_uri.query)
        api_key = source_fields.get("api_key")

        if not api_key or not api_key[0]:
            raise MissingValueError("api_key", "Fireflies")

        # Parse granularity from table name (e.g., analytics:DAY, analytics:HOUR)
        base_table = table
        granularity = None
        if ":" in table:
            parts = table.split(":", 1)
            base_table = parts[0]
            granularity = parts[1].upper()
            if granularity not in {"DAY", "HOUR", "MONTH"}:
                raise ValueError(
                    f"Invalid granularity '{granularity}'. Supported: DAY, HOUR, MONTH"
                )

        if base_table not in {
            "active_meetings",
            "analytics",
            "channels",
            "users",
            "transcripts",
            "user_groups",
            "bites",
            "contacts",
        }:
            raise UnsupportedResourceError(table, "Fireflies")

        if granularity and base_table != "analytics":
            raise ValueError(
                f"Granularity is only supported for 'analytics' table, not '{base_table}'"
            )

        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")

        from omniload.source.fireflies.adapter import fireflies_source

        return fireflies_source(
            api_key=api_key[0],
            start_datetime=interval_start,
            end_datetime=interval_end,
            granularity=granularity,
        ).with_resources(base_table)
