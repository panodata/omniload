class UnsupportedEndpointError(Exception):
    pass


class MissingDecoderError(UnsupportedEndpointError):
    """A routable format resolved to a reader whose decoder package is not installed.

    Raised (instead of a bare ``ImportError``) when an iterable-backed format such as
    ``msgpack`` is requested but the optional ``iterable`` extra / its per-format decoder is
    absent. Carries the exact ``pip install`` target so the message is actionable.
    """

    pass


class MissingReaderOptionError(ValueError):
    """A file reader needs a per-URI ``#key=value`` hint that was not supplied.

    Raised (instead of the connection-shaped ``MissingValueError`` or a bare ``AttributeError``
    from the decoder) when a format requires a reader option that must arrive via the
    ``#key=value`` fragment. XML, for instance, needs ``#tagname=<row-tag>`` to know which
    repeated element is a row. Subclasses ``ValueError`` so it reads as a bad-argument error.
    """

    def __init__(self, option: str, file_format: str, example: str):
        super().__init__(
            f"The {file_format} reader requires a '{option}' hint naming the repeated row "
            f"element. Append it to the source URI as a #{option}=<value> fragment, "
            f"e.g. {example}"
        )
