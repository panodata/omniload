class UnsupportedEndpointError(Exception):
    pass


class MissingDecoderError(UnsupportedEndpointError):
    """A routable format resolved to a reader whose decoder package is not installed.

    Raised (instead of a bare ``ImportError``) when an iterable-backed format such as
    ``msgpack`` is requested but the optional ``iterable`` extra / its per-format decoder is
    absent. Carries the exact ``pip install`` target so the message is actionable.
    """
