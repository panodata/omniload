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

from testcontainers.core.container import DockerContainer
from testcontainers.core.utils import raise_for_deprecated_parameter
from testcontainers.core.wait_strategies import HttpWaitStrategy


class GCSFakeServerContainer(DockerContainer):
    """
    The example below spins up the fake-gcs-server container and
    shows an example to create a Blob service client with the container. The method
    :code:`get_endpoint_url` can be used to get the URL needed to connect
    using the `google.cloud.storage` client.

    https://github.com/fsouza/fake-gcs-server

    Example:

        .. doctest::

            >>> import google.cloud.storage
            >>> from google.auth.credentials import AnonymousCredentials
            >>> from testcontainers.gcs import GCSFakeServerContainer

            >>> with GCSFakeServerContainer() as container:
            ...   endpoint_url = container.get_endpoint_url()
            ...   client = google.cloud.storage.Client(
            ...     credentials=AnonymousCredentials(),
            ...     project="test",
            ...     client_options={"api_endpoint": endpoint_url},
            ...   )
            ...   bucket = client.create_bucket("test-bucket")
            ...   blob = bucket.blob("path/to/file.csv")
            ...   blob.upload_from_filename("path/to/file.csv")

    """

    def __init__(
        self,
        image: str = "docker.io/fsouza/fake-gcs-server:latest",
        *,
        service_port: int = 4443,
        **kwargs,
    ) -> None:
        """Construct a GCSFakeServerContainer.

        Args:
            image: Image name with tag.
            **kwargs: Keyword arguments passed to super class.
        """
        super().__init__(image=image, **kwargs)

        raise_for_deprecated_parameter(
            kwargs, "ports_to_expose", "container.with_exposed_ports"
        )
        self.service_port = service_port

        self.with_env("FAKE_GCS_SCHEME", "http")
        self.with_exposed_ports(service_port)
        self.waiting_for(
            HttpWaitStrategy(service_port, "/storage/v1/b").for_status_code(200)
        )

    def get_endpoint_url(
        self,
    ) -> str:
        """Retrieves the appropriate endpoint URL for the GCSFakeServerContainer container."""
        host_ip = self.get_container_host_ip()
        port = self.get_exposed_port(self.service_port)
        return f"http://{host_ip}:{port}"
