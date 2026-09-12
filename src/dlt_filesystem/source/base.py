from typing import Union
from urllib.parse import urlparse


class FilesystemSource:
    """Shared capabilities for the filesystem-family sources.

    Covers the local ``file://`` source and every remote transport
    (``s3://``, ``gs://``, ``az://`` / ``adls://`` / ``abfss://``, ``sftp://``),
    which all converge on the same reader after URI parsing.

    Filesystem sources manage their own incremental behaviour
    (``handles_incrementality`` is ``True``) and support opt-in file selection by
    modification time (``supports_filesystem_incremental`` is ``True``). They
    carry no resource-level write disposition, so a run-level disposition is safe
    to apply: ``run_ingest`` honours an explicit ``--incremental-strategy append``
    / ``replace`` for them (``honours_run_disposition`` is ``True``). Sources that
    set their own resource-level disposition leave this ``False`` (the default)
    so the run-level value never overrides theirs.
    """

    def handles_incrementality(self) -> bool:
        return True

    def honours_run_disposition(self) -> bool:
        return True

    def consumed_run_options(self) -> frozenset:
        """Return the run options this source's ``dlt_source`` accepts by name.

        Every other name in omniload's run vocabulary is filtered out before the
        call, so it never reaches an fsspec or Arrow constructor as a stray
        keyword. The two named here are resource options, not connector ones:
        they configure how the reader resource is built (``FilesystemReference``)
        and every ``dlt_source`` implementation in this family declares them
        explicitly rather than reading them out of ``**kwargs``.
        """
        return frozenset({"filesystem_incremental", "column_types"})

    def supports_filesystem_incremental(self) -> bool:
        """Return whether the source supports file-level mtime selection."""
        return True

    def produces_multiple_tables(self, uri: str, table: str) -> bool:
        """Return whether a workbook selection dispatches worksheet tables."""
        from dlt_filesystem.source.error import UnsupportedEndpointError
        from dlt_filesystem.source.format.readers import (
            spreadsheet_selection_is_plural,
        )
        from dlt_filesystem.source.router import (
            blob_hints,
            determine_endpoint,
            parse_uri,
        )

        parsed_uri = urlparse(uri)
        _, path = parse_uri(parsed_uri, table)
        try:
            endpoint = determine_endpoint(table, path)
        except (UnsupportedEndpointError, ValueError):
            return False
        return endpoint in {
            "read_excel",
            "read_ods",
        } and spreadsheet_selection_is_plural(blob_hints(parsed_uri, table))

    @staticmethod
    def endpoint_namespace(endpoint: Union[str, None], default: str) -> str:
        """
        Return a normalized endpoint identity without credentials or query values.
        It is used for incremental loading based on file modification times.

        # TODO: Remove `default` argument again?
        """
        if not endpoint:
            return default

        parsed = urlparse(endpoint if "://" in endpoint else f"//{endpoint}")
        host = parsed.hostname
        if not host:
            return default

        host = host.lower()
        if ":" in host:
            host = f"[{host}]"
        if parsed.port is not None:
            host = f"{host}:{parsed.port}"

        return f"{host}{parsed.path.rstrip('/')}"
