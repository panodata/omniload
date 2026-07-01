import sys
from typing import Optional

from tests.util.container.model import DockerService


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
            # SQL Server 2025 intermittently exits between create and readiness on CI;
            # it is a source-only engine-version check, not omniload code under test, so
            # a boot failure skips its ~24 params rather than failing the whole suite.
            optional=True,
        )
    except Exception as exc:
        print(
            f"Failed to load adapter for Microsoft SQL server: {exc}", file=sys.stderr
        )
