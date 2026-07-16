from urllib.parse import parse_qs, urlparse

from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError, UnsupportedResourceError


class PhantombusterSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Phantombuster takes care of incrementality on its own, you should not provide incremental_key"
            )

        # phantombuster://?api_key=<api_key>
        # source table = phantom_results:agent_id
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)
        api_key = params.get("api_key")
        if api_key is None:
            raise MissingValueError("api_key", "Phantombuster")

        table_fields = table.replace(" ", "").split(":")
        table_name = table_fields[0]

        agent_id = table_fields[1] if len(table_fields) > 1 else None

        if table_name not in ["completed_phantoms"]:
            raise UnsupportedResourceError(table_name, "Phantombuster")

        if not agent_id:
            raise MissingValueError("agent_id", "Phantombuster")

        start_date = kwargs.get("interval_start")
        if start_date is None:
            start_date = ensure_pendulum_datetime_utc("2018-01-01").in_tz("UTC")
        else:
            start_date = ensure_pendulum_datetime_utc(start_date).in_tz("UTC")

        end_date = kwargs.get("interval_end")
        if end_date is not None:
            end_date = ensure_pendulum_datetime_utc(end_date).in_tz("UTC")

        from omniload.source.phantombuster.adapter import phantombuster_source

        return phantombuster_source(
            api_key=api_key[0],
            agent_id=agent_id,
            start_date=start_date,
            end_date=end_date,
        ).with_resources(table_name)
