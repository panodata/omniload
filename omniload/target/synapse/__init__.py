import dlt

from omniload.target.model import GenericSqlDestination


class SynapseDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        return dlt.destinations.synapse(credentials=uri, **kwargs)
