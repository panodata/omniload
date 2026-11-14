from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

from dlt.common.time import ensure_pendulum_datetime_utc

from omniload.error import MissingValueError


class ImapSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "IMAP takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)
        host = params.get("host")
        username = params.get("username")
        password = params.get("password")
        start_date = params.get("start_date")

        if host is None:
            raise MissingValueError("host", "IMAP")
        if username is None:
            raise MissingValueError("username", "IMAP")
        if password is None:
            raise MissingValueError("password", "IMAP")

        from omniload.source.imap.adapter import inbox_source

        kwargs: Dict[str, Any] = {}
        if start_date is not None:
            kwargs["start_date"] = ensure_pendulum_datetime_utc(start_date[0])

        return inbox_source(
            host=host[0],
            email_account=username[0],
            password=password[0],
            **kwargs,
        )
