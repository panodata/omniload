import dlt

from omniload.target.model import GenericSqlDestination


class RedshiftDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        return dlt.destinations.redshift(
            credentials=uri.replace("redshift://", "postgresql://"), **kwargs
        )
