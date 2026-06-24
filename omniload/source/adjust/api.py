from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc


class AdjustSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key") and not table.startswith("custom:"):
            raise ValueError(
                "Adjust takes care of incrementality on its own, you should not provide incremental_key"
            )

        source_part = urlparse(uri)
        source_params = parse_qs(source_part.query)
        api_key = source_params.get("api_key")

        if not api_key:
            raise ValueError("api_key in the URI is required to connect to Adjust")

        lookback_days = int(source_params.get("lookback_days", [30])[0])

        start_date = (
            pendulum.now()
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .subtract(days=lookback_days)
        )
        if kwargs.get("interval_start"):
            start_date = (
                ensure_pendulum_datetime_utc(str(kwargs.get("interval_start")))
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .subtract(days=lookback_days)
            )

        end_date = pendulum.now()
        if kwargs.get("interval_end"):
            end_date = ensure_pendulum_datetime_utc(str(kwargs.get("interval_end")))

        from omniload.source.adjust.adapter import (
            REQUIRED_CUSTOM_DIMENSIONS,
            adjust_source,
        )
        from omniload.source.adjust.helpers import parse_filters

        dimensions = None
        metrics = None
        filters = {}
        if table.startswith("custom:"):
            fields = table.split(":", 3)
            if len(fields) != 3 and len(fields) != 4:
                raise ValueError(
                    "Invalid Adjust custom table format. Expected format: custom:<dimensions>,<metrics> or custom:<dimensions>:<metrics>:<filters>"
                )

            dimensions = fields[1].split(",")
            metrics = fields[2].split(",")
            table = "custom"

            found = False
            for dimension in dimensions:
                if dimension in REQUIRED_CUSTOM_DIMENSIONS:
                    found = True
                    break

            if not found:
                raise ValueError(
                    f"At least one of the required dimensions is missing for custom Adjust report: {REQUIRED_CUSTOM_DIMENSIONS}"
                )

            if len(fields) == 4:
                filters_raw = fields[3]
                filters = parse_filters(filters_raw)

        src = adjust_source(
            start_date=start_date,
            end_date=end_date,
            api_key=api_key[0],
            dimensions=dimensions,
            metrics=metrics,
            merge_key=kwargs.get("merge_key"),
            filters=filters,
        )

        return src.with_resources(table)
