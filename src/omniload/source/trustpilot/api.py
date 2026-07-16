from urllib.parse import parse_qs, urlparse

from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class TrustpilotSource:
    # trustpilot://<business_unit_id>?api_key=<api_key>
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Trustpilot takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        business_unit_id = parsed_uri.netloc
        params = parse_qs(parsed_uri.query)

        if not business_unit_id:
            raise MissingValueError("business_unit_id", "Trustpilot")

        api_key = params.get("api_key")
        if api_key is None:
            raise MissingValueError("api_key", "Trustpilot")

        start_date = kwargs.get("interval_start")
        if start_date is None:
            start_date = (
                ensure_pendulum_datetime_utc("2000-01-01").in_tz("UTC").isoformat()
            )
        else:
            start_date = (
                ensure_pendulum_datetime_utc(start_date).in_tz("UTC").isoformat()
            )

        end_date = kwargs.get("interval_end")

        if end_date is not None:
            end_date = ensure_pendulum_datetime_utc(end_date).in_tz("UTC").isoformat()

        if table not in ["reviews"]:
            raise UnsupportedResourceError(table, "Trustpilot")

        from omniload.source.trustpilot.adapter import trustpilot_source

        return trustpilot_source(
            business_unit_id=business_unit_id,
            api_key=api_key[0],
            start_date=start_date,
            end_date=end_date,
        ).with_resources(table)
