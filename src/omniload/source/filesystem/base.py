class FilesystemSource:
    """Shared capabilities for the filesystem-family sources.

    Covers the local ``file://`` source and every remote transport
    (``s3://``, ``gs://``, ``az://`` / ``adls://`` / ``abfss://``, ``sftp://``),
    which all converge on the same reader after URI parsing.

    These sources scan a set of files on each run and cannot derive a per-file
    incremental key, so they manage incrementality themselves
    (``handles_incrementality`` is ``True``). Unlike the SaaS/streaming sources
    that also manage their own incrementality, they carry no resource-level
    write disposition, so a run-level disposition is safe to apply: ``run_ingest``
    honours an explicit ``--incremental-strategy append`` / ``replace`` for them
    (``honours_run_disposition`` is ``True``). Sources that set their own
    resource-level disposition leave this ``False`` (the default) so the run-level
    value never overrides theirs.
    """

    def handles_incrementality(self) -> bool:
        return True

    def honours_run_disposition(self) -> bool:
        return True
