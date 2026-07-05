from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import requests


class MissingValueError(Exception):
    def __init__(self, value, source):
        super().__init__(f"{value} is required to connect to {source}")


class UnsupportedResourceError(Exception):
    def __init__(self, resource, source):
        super().__init__(
            f"Resource '{resource}' is not supported for {source} source yet. "
            f"Please create a GitHub issue at https://github.com/panodata/omniload"
        )


class InvalidBlobTableError(Exception):
    def __init__(self, source):
        super().__init__(
            f"Invalid source table for {source} "
            "Ensure that the table is in the format {bucket-name}/{file glob}"
        )


class HTTPError(Exception):
    def __init__(self, source: requests.HTTPError):
        super().__init__(f"HTTP {source.response.status_code}: {source.response.text}")


class ProcessingError(Exception):
    def __init__(self, message, source):
        super().__init__(f"Processing error in {source} source: {message}")


class ValidationError(Exception):
    """Raised when ingest parameters are invalid (bad table spec, unsupported
    loader file format or column type). The CLI translates this into an abort."""


class IngestJobError(Exception):
    """Raised when one or more dlt load jobs fail. Carries the failed jobs so
    library callers can inspect them; the CLI translates this into exit code 1."""

    def __init__(self, failed_jobs):
        self.failed_jobs = failed_jobs
        details = ", ".join(
            f"{job.job_file_info.job_id()}: {job.failed_message}" for job in failed_jobs
        )
        super().__init__(f"{len(failed_jobs)} load job(s) failed: {details}")
