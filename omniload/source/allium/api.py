from urllib.parse import parse_qs, urlparse

import pendulum

from omniload.error import MissingValueError


class AlliumSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        api_key = query_params.get("api_key")

        if api_key is None:
            raise MissingValueError("api_key", "Allium")

        # Extract query_id and custom parameters from table parameter
        # Format: query_id or query:query_id or query:query_id:param1=value1&param2=value2
        query_id = table
        custom_params = {}
        limit = None
        compute_profile = None

        if ":" in table:
            parts = table.split(":", 2)  # Split into max 3 parts
            if len(parts) >= 2:
                query_id = parts[1]
            if len(parts) == 3:
                # Parse custom parameters from query string format
                param_string = parts[2]
                for param in param_string.split("&"):
                    if "=" in param:
                        key, value = param.split("=", 1)
                        # Extract run_config parameters
                        if key == "limit":
                            limit = int(value)
                        elif key == "compute_profile":
                            compute_profile = value
                        else:
                            custom_params[key] = value

        # Extract parameters from interval_start and interval_end
        # Default: 2 days ago 00:00 to yesterday 00:00
        now = pendulum.now()
        default_start = now.subtract(days=2).start_of("day")
        default_end = now.subtract(days=1).start_of("day")

        parameters = {}
        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")

        start_date = interval_start if interval_start is not None else default_start
        end_date = interval_end if interval_end is not None else default_end

        parameters["start_date"] = start_date.strftime("%Y-%m-%d")
        parameters["end_date"] = end_date.strftime("%Y-%m-%d")
        parameters["start_timestamp"] = str(int(start_date.timestamp()))
        parameters["end_timestamp"] = str(int(end_date.timestamp()))

        # Merge custom parameters (they override default parameters)
        parameters.update(custom_params)

        from omniload.source.allium.adapter import allium_source

        return allium_source(
            api_key=api_key[0],
            query_id=query_id,
            parameters=parameters if parameters else None,
            limit=limit,
            compute_profile=compute_profile,
        )
