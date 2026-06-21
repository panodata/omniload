def test_version_cmd():
    """
    This should always be 0.0.0-dev.
    """
    from verlib2 import Version  # type: ignore[import-untyped]

    from omniload import __version__

    assert Version(__version__) >= Version("0.0.0-dev")
