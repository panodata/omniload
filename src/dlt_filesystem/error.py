from __future__ import annotations


class MissingConnectorOption(Exception):
    def __init__(self, option, connector):
        super().__init__(f"{option} is required to connect to {connector}")


class InvalidBlobTableError(Exception):
    def __init__(self, source):
        super().__init__(
            f"Invalid source table for: {source}. "
            "Ensure that the table is in the format {bucket-name}/{file glob}"
        )
