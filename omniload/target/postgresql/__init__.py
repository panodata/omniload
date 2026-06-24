import dlt

from omniload.target.model import GenericSqlDestination


class PostgresDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        return dlt.destinations.postgres(credentials=uri, **kwargs)
