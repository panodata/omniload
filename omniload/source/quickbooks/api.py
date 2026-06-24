from urllib.parse import parse_qs, urlparse

from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError


class QuickBooksSource:
    def handles_incrementality(self) -> bool:
        return True

    # quickbooks://?company_id=<company_id>&client_id=<client_id>&client_secret=<client_secret>&refresh_token=<refresh>&access_token=<access_token>&environment=<env>&minor_version=<version>
    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "QuickBooks takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)

        params = parse_qs(parsed_uri.query)
        company_id = params.get("company_id")
        client_id = params.get("client_id")
        client_secret = params.get("client_secret")
        refresh_token = params.get("refresh_token")
        environment = params.get("environment", ["production"])
        minor_version = params.get("minor_version", [None])

        if not client_id or not client_id[0].strip():
            raise MissingValueError("client_id", "QuickBooks")

        if not client_secret or not client_secret[0].strip():
            raise MissingValueError("client_secret", "QuickBooks")

        if not refresh_token or not refresh_token[0].strip():
            raise MissingValueError("refresh_token", "QuickBooks")

        if not company_id or not company_id[0].strip():
            raise MissingValueError("company_id", "QuickBooks")

        if environment[0] not in ["production", "sandbox"]:
            raise ValueError(
                "Invalid environment. Must be either 'production' or 'sandbox'."
            )

        from omniload.source.quickbooks.adapter import quickbooks_source

        table_name = table.replace(" ", "")
        table_mapping = {
            "customers": "customer",
            "invoices": "invoice",
            "accounts": "account",
            "vendors": "vendor",
            "payments": "payment",
        }
        if table_name in table_mapping:
            table_name = table_mapping[table_name]

        start_date = kwargs.get("interval_start")
        if start_date is None:
            start_date = ensure_pendulum_datetime_utc("2025-01-01").in_tz("UTC")
        else:
            start_date = ensure_pendulum_datetime_utc(start_date).in_tz("UTC")

        end_date = kwargs.get("interval_end")

        if end_date is not None:
            end_date = ensure_pendulum_datetime_utc(end_date).in_tz("UTC")

        return quickbooks_source(
            company_id=company_id[0],
            start_date=start_date,
            end_date=end_date,
            client_id=client_id[0],
            client_secret=client_secret[0],
            refresh_token=refresh_token[0],
            environment=environment[0],
            minor_version=minor_version[0],
            object=table_name,
        ).with_resources(table_name)
