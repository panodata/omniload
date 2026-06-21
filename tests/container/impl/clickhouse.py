from typing import cast

from testcontainers.core.generic import DbContainer

from tests.container.model import DockerService


class ClickhouseService(DockerService):
    def get_connection_url(self):
        if self.container is None:
            raise ValueError("Container is not initialized.")
        port = self.container.get_exposed_port(8123)
        return (
            cast(DbContainer, self.container)
            .get_connection_url()
            .replace("clickhouse://", "clickhouse+native://")
            + f"?http_port={port}&secure=0"
        )
