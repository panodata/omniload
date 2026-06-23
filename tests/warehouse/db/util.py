import csv
from pathlib import Path
from typing import Union


def assert_output_equals_to_csv(results, path: Union[str, Path]):
    path = Path(path)
    actual_rows = []
    with open(path.absolute(), "r") as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        next(reader, None)
        for row in reader:
            actual_rows.append(row)

    assert len(results) == len(actual_rows)
    for i, row in enumerate(actual_rows):
        assert results[i] == tuple(row)
