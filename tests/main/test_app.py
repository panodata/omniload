def test_version_cmd():
    """
    This should always be 0.0.0-dev.
    """
    from verlib2 import Version  # type: ignore[import-untyped]

    from omniload import __version__

    assert Version(__version__) >= Version("0.0.0-dev")


def test_example_uris_cmd_lists_streaming_schemes(caplog):
    import logging

    from typer.testing import CliRunner

    from omniload.main import app

    with caplog.at_level(logging.INFO, logger="omniload.main"):
        result = CliRunner().invoke(app, ["example-uris"])

    assert result.exit_code == 0
    messages = "\n".join(record.message for record in caplog.records)
    assert "kafka+mqb://localhost:9092" in messages
    assert "nats/amqp/mqtt/zeromq/aws/ibmmq/memory" in messages
