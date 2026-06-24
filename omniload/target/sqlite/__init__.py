import dlt

from omniload.target.model import GenericSqlDestination


class SqliteDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        return dlt.destinations.sqlalchemy(credentials=uri)

    def dlt_run_params(self, uri: str, table: str, **kwargs):
        return {
            # https://dlthub.com/docs/dlt-ecosystem/destinations/sqlalchemy#dataset-files
            "dataset_name": "main",
            "table_name": table,
        }
