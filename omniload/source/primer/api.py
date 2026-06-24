from urllib.parse import parse_qs, urlparse

from omniload.error import MissingValueError, UnsupportedResourceError


class PrimerSource:
    # primer://?api_key=<api_key>
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        api_key = params.get("api_key")
        if api_key is None:
            raise MissingValueError("api_key", "Primer")
        if table not in ["payments"]:
            raise UnsupportedResourceError(table, "Primer")

        date_args: dict[str, str] = {}
        if kwargs.get("interval_start"):
            date_args["start_date"] = kwargs["interval_start"]
        if kwargs.get("interval_end"):
            date_args["end_date"] = kwargs["interval_end"]

        from omniload.source.primer.adapter import primer_source

        return primer_source(
            api_key=api_key[0],
            **date_args,
        ).with_resources(table)
