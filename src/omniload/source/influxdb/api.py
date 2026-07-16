from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError


class InfluxDBSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "InfluxDB takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)
        host = parsed_uri.hostname
        port = parsed_uri.port

        secure = params.get("secure", ["true"])[0].lower() != "false"
        scheme = "https" if secure else "http"

        if port:
            host_url = f"{scheme}://{host}:{port}"
        else:
            host_url = f"{scheme}://{host}"

        token = params.get("token")
        org = params.get("org")
        bucket = params.get("bucket")

        if not host:
            raise MissingValueError("host", "InfluxDB")
        if not token:
            raise MissingValueError("token", "InfluxDB")
        if not org:
            raise MissingValueError("org", "InfluxDB")
        if not bucket:
            raise MissingValueError("bucket", "InfluxDB")

        start_date = kwargs.get("interval_start")
        if start_date is not None:
            start_date = ensure_pendulum_datetime_utc(start_date)
        else:
            start_date = pendulum.datetime(2024, 1, 1).in_tz("UTC")

        end_date = kwargs.get("interval_end")
        if end_date is not None:
            end_date = ensure_pendulum_datetime_utc(end_date)

        from omniload.source.influxdb.adapter import influxdb_source

        return influxdb_source(
            measurement=table,
            host=host_url,
            org=org[0],
            bucket=bucket[0],
            token=token[0],
            secure=secure,
            start_date=start_date,
            end_date=end_date,
        ).with_resources(table)
