#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import functools as ft
import os
import re
from typing import Any, Optional

import boto3
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.core.waiting_utils import wait_for_logs


class FlociContainer(DockerContainer):
    """
    A Testcontainer for Floci with improved configurability.

    It honors the `TC_KEEPALIVE` and `FLOCI_VERSION` environment variables.

    Defining `TC_KEEPALIVE` sets a signal to not shut down the container
    after running the test cases, to speed up later invocations.

    `FLOCI_VERSION` defines the designated Floci version, which is useful when
    used within a test matrix.
    """

    # Pinned to a released tag rather than `latest`. Floci is a young project;
    # bump after verifying the Kinesis and DynamoDB suites pass against the new image.
    FLOCI_VERSION = os.environ.get("FLOCI_VERSION", "1.5.25")

    #: The host's Docker socket, which Floci needs to run Lambda functions.
    DOCKER_SOCKET = "/var/run/docker.sock"

    def __init__(
        self,
        image: str = f"floci/floci:{FLOCI_VERSION}",
        edge_port: int = 4566,
        region_name: Optional[str] = None,
        *,
        docker_socket: bool = False,
        lambda_network: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        `docker_socket` mounts the host's Docker socket, which Floci uses to start
        a container per Lambda function. It hands Floci control of the Docker
        daemon, so it is off by default and only the Lambda tests turn it on.

        `lambda_network` names an existing user-defined Docker network. Floci
        joins it and starts its function containers on it, and they reach Floci
        by the container name, which `name` must therefore set.
        """
        super().__init__(image=image, **kwargs)
        self.edge_port = edge_port
        self.region_name = region_name or os.environ.get(
            "AWS_DEFAULT_REGION", "us-west-1"
        )
        self.with_exposed_ports(self.edge_port)
        self.with_env("AWS_DEFAULT_REGION", self.region_name)
        self.with_env("AWS_ACCESS_KEY_ID", "testcontainers-localstack")
        self.with_env("AWS_SECRET_ACCESS_KEY", "testcontainers-localstack")
        if docker_socket:
            self.with_volume_mapping(self.DOCKER_SOCKET, self.DOCKER_SOCKET, "rw")
            # Whoever reaches the edge port can have Floci start containers, so
            # publish it on the loopback interface only, not on every interface.
            self.ports[str(self.edge_port)] = ("127.0.0.1", None)  # ty: ignore[invalid-assignment]
        if lambda_network is not None:
            if not kwargs.get("name"):
                raise ValueError(
                    "lambda_network needs a container name: function containers "
                    "reach Floci by that name"
                )
            # `with_kwargs` replaces the extra `docker run` arguments, so keep
            # the ones the caller passed (labels, say).
            self.with_kwargs(**{**self._kwargs, "network": lambda_network})
            self.with_env("FLOCI_HOSTNAME", kwargs["name"])
            self.with_env("FLOCI_SERVICES_LAMBDA_DOCKER_NETWORK", lambda_network)

    def _configure(self):
        self.waiting_for(
            LogMessageWaitStrategy(re.compile(r"AWS Local Emulator Ready"))
        )

    def _connect(self):
        """
        Wait for Floci to be fully ready.

        ``KeepaliveContainer.start()`` calls ``_configure()`` and ``_connect()``
        hooks. Without this, Kinesis and DynamoDB service APIs can receive
        requests before Floci is ready.
        """
        if not self._wait_strategy:
            raise ValueError("No wait strategy defined")
        wait_for_logs(self, predicate=self._wait_strategy, timeout=60)

    def get_url(self) -> str:
        """
        Return the mapped Floci edge endpoint.
        """
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.edge_port)
        return f"http://{host}:{port}"

    @ft.wraps(boto3.client)
    def get_client(self, name, **kwargs) -> Any:
        kwargs_ = {
            "endpoint_url": self.get_url(),
            "region_name": self.region_name,
            "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "aws_secret_access_key": "dummy",
        }
        kwargs_.update(kwargs)
        return boto3.client(name, **kwargs_)
