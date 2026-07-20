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


def test_ingest_help_lists_filesystem_incremental_option():
    from typing import Any, cast

    from typer.main import get_command

    from omniload.main import app

    root = cast(Any, get_command(app))
    ingest = root.commands["ingest"]
    option = next(
        parameter
        for parameter in ingest.params
        if "--filesystem-incremental" in parameter.opts
    )

    assert "--no-filesystem-incremental" in option.secondary_opts
    assert option.help is not None
    assert "requires append loading and durable" in option.help
