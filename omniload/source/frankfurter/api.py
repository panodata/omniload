from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import UnsupportedResourceError


class FrankfurterSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Frankfurter takes care of incrementality on its own, you should not provide incremental_key"
            )

        from omniload.source.frankfurter.adapter import frankfurter_source
        from omniload.source.frankfurter.helpers import (
            validate_currency,
            validate_dates,
        )

        parsed_uri = urlparse(uri)
        source_params = parse_qs(parsed_uri.query)
        base_currency = source_params.get("base", [None])[0]

        if not base_currency:
            base_currency = "USD"

        validate_currency(base_currency)

        if kwargs.get("interval_start"):
            start_date = ensure_pendulum_datetime_utc(str(kwargs.get("interval_start")))
        else:
            start_date = pendulum.yesterday()

        if kwargs.get("interval_end"):
            end_date = ensure_pendulum_datetime_utc(str(kwargs.get("interval_end")))
        else:
            end_date = None

        validate_dates(start_date=start_date, end_date=end_date)

        src = frankfurter_source(
            start_date=start_date,
            end_date=end_date,
            base_currency=base_currency,
        )

        if table not in src.resources:
            raise UnsupportedResourceError(table, "Frankfurter")

        return src.with_resources(table)
