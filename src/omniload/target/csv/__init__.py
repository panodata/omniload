"""``csv://``: a CSV-only compatibility spelling of the local ``file://`` destination.

The scheme predates the filesystem family and is kept so existing commands resolve.
Staging and writing come from
:class:`~dlt_filesystem.target.local.LocalFilesystemDestination`, which reads every
staged file of a load in a stable order and always clears its temp directory. See #301.
"""

from dlt_filesystem.target.local import LocalFilesystemDestination
from omniload.core.tablename import two_level


class CsvDestination(LocalFilesystemDestination):
    """Write a dlt load result into a single local CSV file.

    Two things separate it from the ``file://`` destination it inherits:

    - the output format is pinned to CSV, so ``csv://out.jsonl`` and ``#parquet`` are
      rejected before staging while an extensionless ``csv://report`` still writes CSV;
    - the destination table keeps the quote-aware ``<schema>.<table>`` parser it was
      given in #300, rather than ``file://``'s plain split and URI-stem default.
    """

    pinned_output_format = "csv"
    table_capability = two_level("csv")

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        """Decode dataset and table name from a qualified ``--dest-table``.

        A dot inside a quoted identifier is one component, and a name that resolves no
        schema says so rather than restating the format. The shared ``post_load()``
        locates dlt's staged output by these two names, so they are recorded on the
        instance as well as returned.
        """
        parsed = self.table_capability.parse(table)
        if parsed.schema is None:
            raise self.table_capability.unresolved_schema_error(table)

        self.dataset_name = parsed.schema
        self.table_name = parsed.table
        return {
            "dataset_name": self.dataset_name,
            "table_name": self.table_name,
        }
