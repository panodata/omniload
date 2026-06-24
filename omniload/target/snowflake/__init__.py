import dlt

from omniload.target.model import GenericSqlDestination


class SnowflakeDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        return dlt.destinations.snowflake(credentials=uri, **kwargs)
