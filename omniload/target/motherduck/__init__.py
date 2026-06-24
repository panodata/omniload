import dlt

from omniload.target.model import GenericSqlDestination


class MotherduckDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(uri)
        query = parse_qs(parsed.query)
        token = query.get("token", [None])[0]
        from dlt.destinations.impl.motherduck.configuration import MotherDuckCredentials

        creds = {
            "password": token,
        }
        database = parsed.path.lstrip("/") or parsed.netloc
        if database:
            creds["database"] = database

        return dlt.destinations.motherduck(MotherDuckCredentials(creds), **kwargs)
