from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc


class HostawaySource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Hostaway takes care of incrementality on its own, you should not provide incremental_key"
            )

        source_parts = urlparse(uri)
        source_params = parse_qs(source_parts.query)
        api_key = source_params.get("api_key")

        if not api_key:
            raise ValueError("api_key in the URI is required to connect to Hostaway")

        match table:
            case "listings":
                resource_name = "listings"
            case "listing_fee_settings":
                resource_name = "listing_fee_settings"
            case "listing_agreements":
                resource_name = "listing_agreements"
            case "listing_pricing_settings":
                resource_name = "listing_pricing_settings"
            case "cancellation_policies":
                resource_name = "cancellation_policies"
            case "cancellation_policies_airbnb":
                resource_name = "cancellation_policies_airbnb"
            case "cancellation_policies_marriott":
                resource_name = "cancellation_policies_marriott"
            case "cancellation_policies_vrbo":
                resource_name = "cancellation_policies_vrbo"
            case "reservations":
                resource_name = "reservations"
            case "finance_fields":
                resource_name = "finance_fields"
            case "reservation_payment_methods":
                resource_name = "reservation_payment_methods"
            case "reservation_rental_agreements":
                resource_name = "reservation_rental_agreements"
            case "listing_calendars":
                resource_name = "listing_calendars"
            case "conversations":
                resource_name = "conversations"
            case "message_templates":
                resource_name = "message_templates"
            case "bed_types":
                resource_name = "bed_types"
            case "property_types":
                resource_name = "property_types"
            case "countries":
                resource_name = "countries"
            case "account_tax_settings":
                resource_name = "account_tax_settings"
            case "user_groups":
                resource_name = "user_groups"
            case "guest_payment_charges":
                resource_name = "guest_payment_charges"
            case "coupons":
                resource_name = "coupons"
            case "webhook_reservations":
                resource_name = "webhook_reservations"
            case "tasks":
                resource_name = "tasks"
            case _:
                raise ValueError(
                    f"Resource '{table}' is not supported for Hostaway source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
                )

        start_date = kwargs.get("interval_start")
        if start_date:
            start_date = ensure_pendulum_datetime_utc(start_date).in_timezone("UTC")
        else:
            start_date = pendulum.datetime(1970, 1, 1).in_timezone("UTC")

        end_date = kwargs.get("interval_end")
        if end_date:
            end_date = ensure_pendulum_datetime_utc(end_date).in_timezone("UTC")

        from omniload.source.hostaway.adapter import hostaway_source

        return hostaway_source(
            api_key=api_key[0],
            start_date=start_date,
            end_date=end_date,
        ).with_resources(resource_name)
