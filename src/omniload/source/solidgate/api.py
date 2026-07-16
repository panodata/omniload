from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.extract.exceptions import ResourcesNotFoundError

from omniload.error import MissingValueError, UnsupportedResourceError


class SolidgateSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Solidgate takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        public_key = query_params.get("public_key")
        secret_key = query_params.get("secret_key")

        if public_key is None:
            raise MissingValueError("public_key", "Solidgate")

        if secret_key is None:
            raise MissingValueError("secret_key", "Solidgate")

        table_name = table.replace(" ", "")

        start_date = kwargs.get("interval_start")
        if start_date is None:
            start_date = pendulum.yesterday().in_tz("UTC")
        else:
            start_date = ensure_pendulum_datetime_utc(start_date).in_tz("UTC")

        end_date = kwargs.get("interval_end")

        if end_date is not None:
            end_date = ensure_pendulum_datetime_utc(end_date).in_tz("UTC")

        from omniload.source.solidgate.adapter import solidgate_source

        try:
            return solidgate_source(
                public_key=public_key[0],
                secret_key=secret_key[0],
                start_date=start_date,
                end_date=end_date,
            ).with_resources(table_name)
        except ResourcesNotFoundError:
            raise UnsupportedResourceError(table_name, "Solidgate")
