"""AWS Lambda handler running one omniload job, defined by the environment.

The function's environment names the job:

- ``OMNILOAD_SOURCE_URI`` and ``OMNILOAD_SOURCE_TABLE``
- ``OMNILOAD_DEST_URI`` and ``OMNILOAD_DEST_TABLE``

Every invocation runs that job once with the ``replace`` strategy, so a
successful rerun over unchanged input leaves the same output. A failed run
raises, which Lambda reports as a function error.
"""

import os

# The image's file system is read-only on Lambda except for /tmp. Point every
# location omniload or dlt writes to there, before importing them.
for _variable, _path in (
    ("HOME", "/tmp/home"),
    ("TMPDIR", "/tmp"),
    ("DLT_DATA_DIR", "/tmp/dlt"),
    ("XDG_CACHE_HOME", "/tmp/cache"),
):
    os.environ[_variable] = _path
    os.makedirs(_path, exist_ok=True)

from omniload.api import run_ingest  # noqa: E402


def handler(event, context):
    """Run the job and return the dlt load ids it produced."""
    info = run_ingest(
        source_uri=os.environ["OMNILOAD_SOURCE_URI"],
        source_table=os.environ["OMNILOAD_SOURCE_TABLE"],
        dest_uri=os.environ["OMNILOAD_DEST_URI"],
        dest_table=os.environ["OMNILOAD_DEST_TABLE"],
        incremental_strategy="replace",
        progress="log",
    )
    return {"load_ids": list(info.loads_ids) if info is not None else []}
