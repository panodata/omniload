import dataclasses
from typing import Optional

from tests.container.model import AbstractService


@dataclasses.dataclass
class ServiceRegistry:
    """Registry for all services."""

    clickhouse: Optional[AbstractService]
    duckdb_source: Optional[AbstractService]
    duckdb_destination: Optional[AbstractService]
    mongodb: Optional[AbstractService]
    mssql: Optional[AbstractService]
    mysql: Optional[AbstractService]
    postgresql: Optional[AbstractService]
