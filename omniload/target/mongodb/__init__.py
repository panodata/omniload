from omniload.src.mongodb import mongodb_insert


class MongoDBDestination:
    def dlt_dest(self, uri: str, **kwargs):
        return mongodb_insert(uri)

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        return {
            "table_name": table,
        }

    def post_load(self):
        pass
