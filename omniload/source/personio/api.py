from urllib.parse import parse_qs, urlparse

from omniload.error import MissingValueError, UnsupportedResourceError


class PersonioSource:
    def handles_incrementality(self) -> bool:
        return True

    # applovin://?client_id=123&client_secret=123
    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Personio takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        client_id = params.get("client_id")
        client_secret = params.get("client_secret")

        interval_start = kwargs.get("interval_start")
        interval_end = kwargs.get("interval_end")

        interval_start_date = (
            interval_start if interval_start is not None else "2018-01-01"
        )

        interval_end_date = (
            interval_end.strftime("%Y-%m-%d") if interval_end is not None else None
        )

        if client_id is None:
            raise MissingValueError("client_id", "Personio")
        if client_secret is None:
            raise MissingValueError("client_secret", "Personio")
        if table not in [
            "employees",
            "absences",
            "absence_types",
            "attendances",
            "projects",
            "document_categories",
            "employees_absences_balance",
            "custom_reports_list",
        ]:
            raise UnsupportedResourceError(table, "Personio")

        from omniload.source.personio.adapter import personio_source

        return personio_source(
            client_id=client_id[0],
            client_secret=client_secret[0],
            start_date=interval_start_date,
            end_date=interval_end_date,
        ).with_resources(table)
