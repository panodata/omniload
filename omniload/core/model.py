import importlib
import typing
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Protocol

if typing.TYPE_CHECKING:
    from dlt.common.destination import Destination


class SourceProtocol(Protocol):
    """
    Note: an optional ``post_load(self) -> None`` hook may be defined by sources that ack
    their own offsets (e.g. mq-bridge). It is intentionally NOT part of this protocol so
    that ordinary sources need not implement it; run_ingest invokes it only when present
    via ``getattr(source, "post_load", lambda: None)()``.
    """  # noqa: E501

    def dlt_source(self, uri: str, table: str, **kwargs):
        """Build the dlt source or resource for the requested table."""
        pass

    def handles_incrementality(self) -> bool:
        """Return whether the source manages its own incremental state."""
        pass


class DestinationProtocol(Protocol):
    """Protocol implemented by destination adapters used by the factory."""

    def dlt_dest(self, uri: str, **kwargs) -> "Destination":
        """Build the dlt destination for the given URI."""
        pass

    def dlt_run_params(self, uri: str, table: str, **kwargs):
        """Return dlt pipeline run parameters for the destination table."""
        pass

    def post_load(self) -> None:
        """Run destination follow-up work after a successful load."""
        pass


@dataclass
class TableDefinition:
    """Parsed schema and table components for a source table specifier."""

    dataset: str
    table: str


def table_string_to_dataclass(table: str) -> TableDefinition:
    """Parse a schema-qualified table string into a table definition."""
    table_fields = table.split(".", 1)
    if len(table_fields) != 2:
        raise ValueError("Table name must be in the format <schema>.<table>")

    return TableDefinition(dataset=table_fields[0], table=table_fields[1])


class LazyRegistry(Mapping):
    """Mapping that imports a connector class only when its scheme is first accessed."""

    def __init__(self, paths: dict[str, str]) -> None:
        """Store scheme-to-class paths without importing connector modules."""
        self._paths = paths
        self._cache: dict[str, Any] = {}

    def _load(self, dotted_path: str) -> Any:
        """Import and return the connector class named by a dotted path."""
        module_path, cls_name = dotted_path.rsplit(":", 1)
        return getattr(importlib.import_module(module_path), cls_name)

    def __getitem__(self, key: str) -> Any:
        """Return the cached connector class for a scheme, importing on first use."""
        if key not in self._cache:
            if key not in self._paths:
                raise KeyError(key)
            self._cache[key] = self._load(self._paths[key])
        return self._cache[key]

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered URI schemes without importing connectors."""
        return iter(self._paths)

    def __len__(self) -> int:
        """Return the number of registered URI schemes."""
        return len(self._paths)

    def __contains__(self, key: object) -> bool:  # O(1), no import triggered
        """Return whether a scheme is registered without importing it."""
        return key in self._paths
