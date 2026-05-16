from importlib.metadata import PackageNotFoundError, version
from unittest import mock

__appname__ = "omniload"


try:
    __version__ = version(__appname__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0-dev"

# Turn off dlt's telemetry to tame three more requests per invocation.
mock.patch("dlt.common.runtime.telemetry._TELEMETRY_STARTED", True)
