from importlib.metadata import PackageNotFoundError, version

__appname__ = "omniload"

try:
    __version__ = version(__appname__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0-dev"
