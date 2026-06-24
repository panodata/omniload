from urllib.parse import parse_qs, urlparse

from dlt.extract.exceptions import ResourcesNotFoundError

from omniload.error import MissingValueError, UnsupportedResourceError


class AttioSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        api_key = query_params.get("api_key")

        if api_key is None:
            raise MissingValueError("api_key", "Attio")

        parts = table.replace(" ", "").split(":")
        table_name = parts[0]
        params = parts[1:]

        from omniload.source.attio.adapter import attio_source

        try:
            return attio_source(api_key=api_key[0], params=params).with_resources(
                table_name
            )
        except ResourcesNotFoundError:
            raise UnsupportedResourceError(table_name, "Attio")
