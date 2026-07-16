from urllib.parse import parse_qs, urlparse

from omniload.error import MissingValueError, UnsupportedResourceError


class BruinSource:
    # bruin://?api_token=<api_token>
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        api_token = params.get("api_token")
        if api_token is None:
            raise MissingValueError("api_token", "Bruin")

        if table not in ["pipelines", "assets"]:
            raise UnsupportedResourceError(table, "Bruin")

        from omniload.source.bruin.adapter import bruin_source

        return bruin_source(api_token=api_token[0]).with_resources(table)
