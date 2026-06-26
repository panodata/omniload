from omniload.core.registry import destinations, sources


def test_connectors():
    """Touch all source- and target-connectors."""
    for protocol, source in sources.items():
        result = source()
        assert hasattr(result, "dlt_source"), f"Source connector failed: {protocol}"

    for protocol, destination in destinations.items():
        result = destination()
        assert hasattr(result, "dlt_dest"), f"Destination connector failed: {protocol}"
