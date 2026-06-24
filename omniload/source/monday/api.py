from urllib.parse import parse_qs, urlparse

from dlt.extract.exceptions import ResourcesNotFoundError

from omniload.error import MissingValueError, UnsupportedResourceError


class MondaySource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        api_token = query_params.get("api_token")

        if api_token is None:
            raise MissingValueError("api_token", "Monday")

        parts = table.replace(" ", "").split(":")
        table_name = parts[0]
        params = parts[1:]

        # Get interval_start and interval_end from kwargs (command line args)
        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")

        # Convert datetime to string format YYYY-MM-DD
        start_date = interval_start.strftime("%Y-%m-%d") if interval_start else None
        end_date = interval_end.strftime("%Y-%m-%d") if interval_end else None

        from omniload.source.monday.adapter import monday_source

        try:
            return monday_source(
                api_token=api_token[0],
                params=params,
                start_date=start_date,
                end_date=end_date,
            ).with_resources(table_name)
        except ResourcesNotFoundError:
            raise UnsupportedResourceError(table_name, "Monday")
