import csv
import io
import json

import pytest
import sqlalchemy

from tests.util import invoke_ingest_command
from tests.util.common import get_random_string, get_testdata_path
from tests.warehouse.settings import DESTINATIONS

TESTDATA = get_testdata_path()

# Three flat rows re-encoded as csv / jsonl / parquet so a single fixture exercises
# every reader the file:// source inherits from the shared FORMAT_TO_READER registry.
PEOPLE = "name,age\nAlice,30\nBob,25\nCarol,41\n"


@pytest.fixture(scope="module")
def people_dir(tmp_path_factory):
    pya_csv = pytest.importorskip("pyarrow.csv")
    pya_parquet = pytest.importorskip("pyarrow.parquet")
    d = tmp_path_factory.mktemp("file_local")
    (d / "people.csv").write_text(PEOPLE)
    with (d / "people.jsonl").open("w") as f:
        for row in csv.DictReader(io.StringIO(PEOPLE)):
            f.write(json.dumps(row) + "\n")
    table = pya_csv.read_csv(io.BytesIO(PEOPLE.encode()))
    pya_parquet.write_table(table, d / "people.parquet")
    return d


def _scalar(dest_uri, sql):
    engine = sqlalchemy.create_engine(dest_uri)
    try:
        with engine.connect() as conn:
            return conn.exec_driver_sql(sql).fetchone()
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_file_local_load(dest, people_dir):
    """Load real local csv/jsonl/parquet files through file:// into each destination."""
    dest_uri = dest.start()
    try:
        schema = f"testschema_file_{get_random_string(5)}"

        # CSV: the file shipped with the repo, the reporter's exact repro path.
        csv_table = f"{schema}.shipped_csv"
        result = invoke_ingest_command(
            f"file://{TESTDATA / 'create_replace.csv'}",
            "create_replace",
            dest_uri,
            csv_table,
        )
        assert result.exit_code == 0
        assert _scalar(dest_uri, f"select count(*) from {csv_table}") == (20,)
        assert _scalar(
            dest_uri, f"select name from {csv_table} where symbol = 'A'"
        ) == ("AGILENT TECHNOLOGIES INC",)

        # JSONL and Parquet: same three rows, different reader.
        for suffix in ("jsonl", "parquet"):
            dest_table = f"{schema}.people_{suffix}"
            result = invoke_ingest_command(
                f"file://{people_dir / f'people.{suffix}'}",
                f"people_{suffix}",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert _scalar(dest_uri, f"select count(*) from {dest_table}") == (3,)
            # jsonl carries age as a string, parquet as int64; normalize across both
            # readers and the destination backends.
            age = _scalar(dest_uri, f"select age from {dest_table} where name = 'Bob'")
            assert str(age[0]) == "25"
    finally:
        dest.stop()
