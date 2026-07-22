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

    def supports_filesystem_incremental(self) -> bool:
        """Return whether the source supports file-level mtime selection."""
        return True

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
