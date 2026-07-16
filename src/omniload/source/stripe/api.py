from typing import Optional
from urllib.parse import parse_qs, urlparse

from dlt.common.time import ensure_pendulum_datetime_utc


class StripeAnalyticsSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Stripe takes care of incrementality on its own, you should not provide incremental_key"
            )

        api_key = None
        source_field = urlparse(uri)
        source_params = parse_qs(source_field.query)
        api_key = source_params.get("api_key")

        if not api_key:
            raise ValueError("api_key in the URI is required to connect to Stripe")

        table = table.lower()

        from omniload.source.stripe.settings import ENDPOINTS

        endpoint = None
        incremental = False
        sync = False

        table_fields = table.split(":")
        if len(table_fields) == 1:
            endpoint = table_fields[0]
        elif len(table_fields) == 2:
            endpoint = table_fields[0]
            sync = table_fields[1] == "sync"
        elif len(table_fields) == 3:
            endpoint = table_fields[0]
            sync = table_fields[1] == "sync"
            incremental = table_fields[2] == "incremental"
        else:
            raise ValueError(
                "Invalid Stripe table format. Expected: stripe:<endpoint> or stripe:<endpoint>:<sync> or stripe:<endpoint>:<sync>:<incremental>"
            )

        if incremental and not sync:
            raise ValueError("incremental loads must be used with sync loading")

        if incremental:
            from omniload.source.stripe.adapter import (
                incremental_stripe_source,
            )

            def nullable_date(date_str: Optional[str]):
                if date_str:
                    return ensure_pendulum_datetime_utc(date_str)
                return None

            endpoint = ENDPOINTS[endpoint]
            return incremental_stripe_source(
                endpoints=(endpoint,),
                stripe_secret_key=api_key[0],
                initial_start_date=nullable_date(kwargs.get("interval_start", None)),
                end_date=nullable_date(kwargs.get("interval_end", None)),
            ).with_resources(endpoint)
        else:
            endpoint = ENDPOINTS[endpoint]
            if sync:
                from omniload.source.stripe.adapter import stripe_source

                return stripe_source(
                    endpoints=(endpoint,),
                    stripe_secret_key=api_key[0],
                ).with_resources(endpoint)
            else:
                from omniload.source.stripe.adapter import async_stripe_source

                return async_stripe_source(
                    endpoints=(endpoint,),
                    stripe_secret_key=api_key[0],
                    max_workers=kwargs.get("extract_parallelism", 4),
                ).with_resources(endpoint)

        raise ValueError(
            f"Resource '{table}' is not supported for stripe source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
        )
