import csv
import os
import shutil
import tempfile

import dlt.destinations

from omniload.target.loader import load_dlt_file
from omniload.target.model import GenericSqlDestination


class CustomCsvDestination(dlt.destinations.filesystem):
    pass


class CsvDestination(GenericSqlDestination):
    temp_path: str
    actual_path: str
    uri: str
    dataset_name: str
    table_name: str

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        table_fields = table.split(".")
        if len(table_fields) != 2:
            raise ValueError("Table name must be in the format <schema>.<table>")

        res = {
            "dataset_name": table_fields[-2],
            "table_name": table_fields[-1],
        }

        self.dataset_name = res["dataset_name"]
        self.table_name = res["table_name"]
        self.uri = uri

        return res

    def dlt_dest(self, uri: str, **kwargs):
        if uri.startswith("csv://"):
            uri = uri.replace("csv://", "file://")

        temp_path = tempfile.mkdtemp()
        self.actual_path = uri
        self.temp_path = temp_path
        return CustomCsvDestination(bucket_url=f"file://{temp_path}", **kwargs)

    # I dislike this implementation quite a bit since it ties the implementation to some internal details on how dlt works
    # I would prefer a custom destination that allows me to do this easily but dlt seems to have a lot of internal details that are not documented
    # I tried to make it work with a nicer destination implementation but I couldn't, so I decided to go with this hack to experiment
    # if anyone has a better idea on how to do this, I am open to contributions or suggestions
    def post_load(self):
        def find_first_file(path):
            for entry in os.listdir(path):
                full_path = os.path.join(path, entry)
                if os.path.isfile(full_path):
                    return full_path

            return None

        def filter_keys(dictionary):
            return {
                key: value
                for key, value in dictionary.items()
                if not key.startswith("_dlt_")
            }

        first_file_path = find_first_file(
            f"{self.temp_path}/{self.dataset_name}/{self.table_name}"
        )

        output_path = self.uri.split("://")[1]
        if output_path.count("/") > 1:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

        def _rewrite_csv_with_fieldnames(path, fieldnames):
            tmp_fd, tmp_path = tempfile.mkstemp(
                suffix=".csv", dir=os.path.dirname(path) or "."
            )
            try:
                os.close(tmp_fd)
                with (
                    open(path, "r", newline="") as old,
                    open(tmp_path, "w", newline="") as new,
                ):
                    reader = csv.DictReader(old)
                    writer = csv.DictWriter(new, fieldnames=fieldnames, restval="")
                    writer.writeheader()
                    for r in reader:
                        writer.writerow(r)
                os.replace(tmp_path, path)
            except BaseException:
                os.unlink(tmp_path)
                raise

        fieldnames = {}
        csv_writer = None
        csv_file = None

        try:
            for row in load_dlt_file(first_file_path):
                row = filter_keys(row)
                new_fields = False
                for key in row:
                    if key not in fieldnames:
                        fieldnames[key] = None
                        new_fields = True

                if csv_writer is None:
                    csv_file = open(output_path, "w", newline="")
                    csv_writer = csv.DictWriter(
                        csv_file, fieldnames=fieldnames, restval=""
                    )
                    csv_writer.writeheader()
                elif new_fields:
                    if csv_file is not None:
                        csv_file.close()
                    _rewrite_csv_with_fieldnames(output_path, list(fieldnames))
                    csv_file = open(output_path, "a", newline="")
                    csv_writer = csv.DictWriter(
                        csv_file, fieldnames=fieldnames, restval=""
                    )

                csv_writer.writerow(row)
        finally:
            if csv_file:
                csv_file.close()
        shutil.rmtree(self.temp_path)
