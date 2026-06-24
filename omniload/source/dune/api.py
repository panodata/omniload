from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.extract.exceptions import ResourcesNotFoundError

from omniload.error import MissingValueError, UnsupportedResourceError


class DuneSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        api_key = query_params.get("api_key")

        if api_key is None:
            raise MissingValueError("api_key", "Dune")

        performance = query_params.get("performance")

        # Extract parameters from interval_start and interval_end
        # Default: 2 days ago 00:00 to yesterday 00:00
        now = pendulum.now()
        default_start = now.subtract(days=2).start_of("day")
        default_end = now.subtract(days=1).start_of("day")

        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")

        start_date = interval_start if interval_start is not None else default_start
        end_date = interval_end if interval_end is not None else default_end

        default_parameters = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "start_timestamp": str(int(start_date.timestamp())),
            "end_timestamp": str(int(start_date.timestamp())),
        }

        from omniload.source.dune.adapter import dune_source

        # "query:<id>" → execute saved query by ID
        # "sql:<raw SQL>" → execute raw SQL
        # otherwise → treat as a resource name (e.g. "queries")
        if table.startswith("query:"):
            parts = table.split(":", 2)
            query_id = parts[1]
            if not query_id:
                raise ValueError("Query ID cannot be empty in 'query:' table format")
            query_parameters = dict(default_parameters)
            if len(parts) == 3:
                query_parameters.update(
                    dict(
                        param.split("=", 1)
                        for param in parts[2].split("&")
                        if "=" in param
                    )
                )

            return dune_source(
                api_key=api_key[0],
                query_id=query_id,
                performance=performance[0] if performance else "medium",
                query_parameters=query_parameters,
            )

        if table.startswith("sql:"):
            sql = table[4:]
            if not sql:
                raise ValueError("SQL query cannot be empty in 'sql:' table format")
            return dune_source(
                api_key=api_key[0],
                sql=sql,
                performance=performance[0] if performance else "medium",
            )

        try:
            return dune_source(
                api_key=api_key[0],
                sql="",
            ).with_resources(table)
        except ResourcesNotFoundError:
            raise UnsupportedResourceError(table, "Dune")
