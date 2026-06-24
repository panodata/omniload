from urllib.parse import parse_qs, urlparse

import pendulum

from omniload.error import MissingValueError


class IsocPulseSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Internet Society Pulse takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)
        token = params.get("token")
        if not token or not token[0].strip():
            raise MissingValueError("token", "Internet Society Pulse")

        start_date = kwargs.get("interval_start")
        if start_date is None:
            start_date = pendulum.now().in_tz("UTC").subtract(days=30)

        end_date = kwargs.get("interval_end")

        metric = table
        opts = []
        if ":" in metric:
            metric, *opts = metric.strip().split(":")
            opts = [opt.strip() for opt in opts]

        from omniload.source.isoc_pulse.adapter import pulse_source

        src = pulse_source(
            token=token[0],
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d") if end_date else None,
            metric=metric,
            opts=opts,
        )
        return src.with_resources(metric)
