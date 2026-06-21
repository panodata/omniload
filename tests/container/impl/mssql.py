import sys
from typing import Optional

from tests.container.model import DockerService


def get_mssql_service(image: str) -> Optional[DockerService]:
    # [unixODBC][Driver Manager]Can't open lib 'ODBC Driver 18 for SQL Server' : file not found (0) (SQLDriverConnect)
    if sys.platform != "linux":
        return None
    try:
        from testcontainers.mssql import SqlServerContainer

        return DockerService(
            "sqlserver",
            lambda: SqlServerContainer(image, dialect="mssql"),
            "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=Yes",
        )
    except Exception as exc:
        print(
            f"Failed to load adapter for Microsoft SQL server: {exc}", file=sys.stderr
        )
