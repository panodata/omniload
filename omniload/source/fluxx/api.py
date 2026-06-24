from urllib.parse import parse_qs, urlparse

from dlt.common.time import ensure_pendulum_datetime_utc


class FluxxSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Fluxx takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Check for malformed URI with embedded scheme
        if "http://" in uri.lower() or "https://" in uri.lower():
            raise ValueError(
                "Invalid Fluxx URI format. Do not include http:// or https:// in the URI."
            )

        # Parse URI: fluxx://instance?client_id=xxx&client_secret=xxx
        parsed_uri = urlparse(uri)
        source_params = parse_qs(parsed_uri.query)

        instance = parsed_uri.hostname
        if not instance:
            raise ValueError(
                "Instance is required in the URI (e.g., fluxx://mycompany.preprod)"
            )

        client_id = source_params.get("client_id")
        if not client_id:
            raise ValueError("client_id in the URI is required to connect to Fluxx")

        client_secret = source_params.get("client_secret")
        if not client_secret:
            raise ValueError("client_secret in the URI is required to connect to Fluxx")

        # Parse date parameters
        start_date = kwargs.get("interval_start")
        if start_date:
            start_date = ensure_pendulum_datetime_utc(start_date)

        end_date = kwargs.get("interval_end")
        if end_date:
            end_date = ensure_pendulum_datetime_utc(end_date)

        # Import Fluxx source
        from omniload.source.fluxx.adapter import fluxx_source

        # Parse table specification for custom column selection
        # Format: "resource_name:field1,field2,field3" or "resource_name"
        resources = None
        custom_fields = {}

        if table:
            # Handle single resource with custom fields or multiple resources
            if ":" in table and table.count(":") == 1:
                # Single resource with custom fields: "grant_request:id,name,amount"
                resource_name, field_list = table.split(":", 1)
                resource_name = resource_name.strip()
                fields = [f.strip() for f in field_list.split(",")]
                resources = [resource_name]
                custom_fields[resource_name] = fields
            else:
                # Multiple resources or single resource without custom fields
                # Support comma-separated list: "grant_request,user"
                resources = [r.strip() for r in table.split(",")]

        return fluxx_source(
            instance=instance,
            client_id=client_id[0],
            client_secret=client_secret[0],
            start_date=start_date,
            end_date=end_date,
            resources=resources,
            custom_fields=custom_fields,
        )
