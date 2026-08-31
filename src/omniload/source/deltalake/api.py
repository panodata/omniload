class DeltaLakeSource:
    """Source adapter for reading from Delta Lake tables."""

    def handles_incrementality(self) -> bool:
        return True

    def honours_run_disposition(self) -> bool:
        """Accept an explicit run-level `--incremental-strategy`.

        The resource carries `replace`, which stands when no strategy is given.
        An explicit `append` or `replace` overrides it; the key-dependent
        strategies are rejected by `run_ingest`, because a full-table read
        exposes no incremental or merge key to build them from.
        """
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        uri = uri.replace("+delta://", "://")

        if kwargs.get("requested_incremental_key"):
            raise ValueError(
                "DeltaLake takes care of incrementality on its own, "
                "you should not provide incremental_key"
            )

        from omniload.source.deltalake.adapter import deltalake_source

        # `--page-size` always arrives with a value, so this is what sets the
        # batch size in a CLI run; the adapter's own default covers a direct call.
        return deltalake_source(uri, table, batch_size=kwargs.get("page_size"))
