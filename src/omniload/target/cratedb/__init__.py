from omniload.target.model import GenericSqlDestination


class CrateDBDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        uri = uri.replace("cratedb://", "postgres://")
        import dlt_cratedb.impl.cratedb.factory

        return dlt_cratedb.impl.cratedb.factory.cratedb(credentials=uri, **kwargs)
