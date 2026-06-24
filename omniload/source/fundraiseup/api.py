from urllib.parse import parse_qs, urlparse

from omniload.error import MissingValueError, UnsupportedResourceError


class FundraiseupSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        api_key = params.get("api_key")
        if api_key is None:
            raise MissingValueError("api_key", "Fundraiseup")

        from omniload.source.fundraiseup.adapter import fundraiseup_source

        src = fundraiseup_source(api_key=api_key[0])
        if table not in src.resources:
            raise UnsupportedResourceError(table, "Fundraiseup")
        return src.with_resources(table)
