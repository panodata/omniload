from dataclasses import dataclass
from typing import Protocol

from dlt.common.destination import Destination


class SourceProtocol(Protocol):
    def dlt_source(self, uri: str, table: str, **kwargs):
        pass

    def handles_incrementality(self) -> bool:
        pass


class DestinationProtocol(Protocol):
    def dlt_dest(self, uri: str, **kwargs) -> Destination:
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
