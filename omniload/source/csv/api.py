import csv
from typing import Any, Dict, Optional

from dlt.extract import Incremental as dlt_incremental


class LocalCsvSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        def csv_file(
            incremental: Optional[dlt_incremental[Any]] = None,
        ):
            file_path = uri.split("://")[1]
            myFile = open(file_path, "r")
            reader = csv.DictReader(myFile)
            if not reader.fieldnames:
                raise RuntimeError(
                    "failed to extract headers from the CSV, are you sure the given file contains a header row?"
                )

            incremental_key = kwargs.get("incremental_key")
            if incremental_key and incremental_key not in reader.fieldnames:
                raise ValueError(
                    f"incremental_key '{incremental_key}' not found in the CSV file"
                )

            page_size = 1000
            page = []
            current_items = 0
            for dictionary in reader:
                # Skip rows where all values are None or empty/whitespace
                if all(
                    v is None or (isinstance(v, str) and v.strip() == "")
                    for v in dictionary.values()
                ):
                    continue

                # Skip rows based on incremental key if specified
                if incremental_key and incremental and incremental.start_value:
                    inc_value = dictionary.get(incremental_key)
                    if inc_value is None:
                        raise ValueError(
                            f"incremental_key '{incremental_key}' not found in the CSV file"
                        )

                    if inc_value < incremental.start_value:
                        continue

                dictionary = self.remove_empty_columns(dictionary)
                page.append(dictionary)
                current_items += 1

                # Yield page when it reaches page_size
                if current_items >= page_size:
                    yield page
                    page = []
                    current_items = 0

            if page:
                yield page

        from dlt import resource

        return resource(  # ty: ignore[no-matching-overload]
            csv_file,
            merge_key=kwargs.get("merge_key"),
        )(
            incremental=dlt_incremental(
                kwargs.get("incremental_key", ""),
                initial_value=kwargs.get("interval_start"),
                end_value=kwargs.get("interval_end"),
                range_end="closed",
                range_start="closed",
            )
        )

    def remove_empty_columns(self, row: Dict[str, str]) -> Dict[str, str]:
        return {k: v for k, v in row.items() if v.strip() != ""}
