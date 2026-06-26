class DeltaLakeSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):

        uri = uri.replace("+delta://", "://")

        # TODO: Review!
        if kwargs.get("incremental_key"):
            raise ValueError(
                "DeltaLake takes care of incrementality on its own, you should not provide incremental_key"
            )

        from omniload.source.deltalake.adapter import deltalake_source

        return deltalake_source(uri, table)
