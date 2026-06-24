import importlib
import typing
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Protocol

if typing.TYPE_CHECKING:
    from dlt.common.destination import Destination


class SourceProtocol(Protocol):
    def dlt_source(self, uri: str, table: str, **kwargs):
        pass

    def handles_incrementality(self) -> bool:
        pass


class DestinationProtocol(Protocol):
    def dlt_dest(self, uri: str, **kwargs) -> "Destination":
        pass

    def dlt_run_params(self, uri: str, table: str, **kwargs):
        pass

    def post_load(self) -> None:
        pass


@dataclass
class TableDefinition:
    dataset: str
    table: str


def table_string_to_dataclass(table: str) -> TableDefinition:
    table_fields = table.split(".", 1)
    if len(table_fields) != 2:
        raise ValueError("Table name must be in the format <schema>.<table>")

    return TableDefinition(dataset=table_fields[0], table=table_fields[1])


class LazyRegistry(Mapping):
    """Mapping that imports a connector class only when its scheme is first accessed."""

    def __init__(self, paths: dict[str, str]) -> None:
        self._paths = paths
        self._cache: dict[str, Any] = {}

    def _load(self, dotted_path: str) -> Any:
        module_path, cls_name = dotted_path.rsplit(":", 1)
        return getattr(importlib.import_module(module_path), cls_name)

    def __getitem__(self, key: str) -> Any:
        if key not in self._cache:
            if key not in self._paths:
                raise KeyError(key)
            self._cache[key] = self._load(self._paths[key])
        return self._cache[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._paths)

    def __len__(self) -> int:
        return len(self._paths)

    def __contains__(self, key: object) -> bool:  # O(1), no import triggered
        return key in self._paths
