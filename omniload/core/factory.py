from urllib.parse import urlparse

from omniload.core.model import DestinationProtocol, LazyRegistry, SourceProtocol
from omniload.core.registry import (
    SQL_SOURCE_SCHEMES,
    destinations,
    sources,
)
from omniload.core.router import SqlSourceRouter


def parse_scheme_from_uri(uri: str) -> str:
    """Return the leading URI scheme used for source and destination dispatch."""
    parsed = urlparse(uri)
    if parsed.scheme != "":
        return parsed.scheme

    uri_parts = uri.split("://")
    if len(uri_parts) > 1:
        return uri_parts[0]

    raise ValueError(f"Could not parse scheme from uri: {uri}")


class SourceDestinationFactory:
    """Resolve source and destination URIs into registered connector instances."""

    source_scheme: str
    destination_scheme: str
    sources: LazyRegistry = sources
    destinations: LazyRegistry = destinations

    def __init__(self, source_uri: str, destination_uri: str):
        """Store URIs and parse their dispatch schemes."""
        self.source_uri = source_uri
        self.source_scheme = parse_scheme_from_uri(source_uri)

        self.destination_uri = destination_uri
        self.destination_scheme = parse_scheme_from_uri(destination_uri)

    def get_source(self) -> SourceProtocol:
        """Build the source connector for the parsed source scheme."""
        if self.source_scheme in SQL_SOURCE_SCHEMES:
            return SqlSourceRouter()
        elif self.source_scheme in self.sources:
            return self.sources[self.source_scheme]()
        else:
            raise ValueError(f"Unsupported source scheme: {self.source_scheme}")

    def get_destination(self) -> DestinationProtocol:
        """Build the destination connector for the parsed destination scheme."""
        if self.destination_scheme in self.destinations:
            return self.destinations[self.destination_scheme]()
        else:
            raise ValueError(
                f"Unsupported destination scheme: {self.destination_scheme}"
            )
