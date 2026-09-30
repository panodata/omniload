"""End-to-end tests for multi-worksheet XLSX and ODS filesystem reads."""

from collections import OrderedDict
from pathlib import Path

import dlt
import duckdb
import pytest
import xlsxwriter
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt_filesystem.source.format.readers import spreadsheet_selection_is_plural

from omniload.api import run_ingest
from omniload.error import ValidationError


def _write_workbook(path: Path, sheets: OrderedDict[str, list[list]]) -> Path:
    if path.suffix == ".xlsx":
        workbook = xlsxwriter.Workbook(path)
        for sheet_name, rows in sheets.items():
            worksheet = workbook.add_worksheet(sheet_name)
            for row_index, row in enumerate(rows):
                worksheet.write_row(row_index, 0, row)
        workbook.close()
    else:
        import pyexcel_ods3

        pyexcel_ods3.save_data(str(path), sheets)
    return path


@pytest.fixture(params=["xlsx", "ods"])
def workbook_file(request, tmp_path) -> Path:
    return _write_workbook(
        tmp_path / f"workbook.{request.param}",
        OrderedDict(
            [
                ("Events", [["id", "kind"], [1, "open"], [2, "closed"]]),
                ("Inventory", [["sku", "quantity"], ["A", 3]]),
                ("Empty", []),
            ]
        ),
    )


def _load_to_duckdb(
    source: str,
    database: Path,
    *,
    dest_table: str = "landing.placeholder",
    schema_naming: str = "default",
):
    return run_ingest(
        source_uri=source,
        dest_uri=f"duckdb:///{database}",
        dest_table=dest_table,
        schema_naming=schema_naming,
        progress="log",
    )


def _user_tables(database: Path, dataset: str = "landing") -> list[str]:
    with duckdb.connect(str(database)) as connection:
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name NOT LIKE '\\_dlt\\_%' ESCAPE '\\'
            ORDER BY table_name
            """,
            [dataset],
        ).fetchall()
    return [row[0] for row in rows]


@pytest.mark.parametrize(
    ("hints", "expected"),
    [
        ({}, True),
        ({"sheet_name": ""}, True),
        ({"sheet_name": "Events"}, False),
        ({"sheet_name": ["Events", "Inventory"]}, True),
        ({"sheet_name": '["Events", "Inventory"]'}, True),
        ({"sheet_id": ""}, True),
        ({"sheet_id": 1}, False),
        ({"sheet_id": "1"}, False),
        ({"sheet_id": 0}, True),
        ({"sheet_id": "0"}, True),
        ({"sheet_id": [1, 2]}, True),
        ({"sheet_id": "[1, 2]"}, True),
        ({"table_name": "NamedRange", "sheet_name": ["Events"]}, False),
    ],
)
def test_spreadsheet_selection_is_plural(hints, expected):
    assert spreadsheet_selection_is_plural(hints) is expected


def test_default_loads_every_nonempty_worksheet(workbook_file, tmp_path):
    database = tmp_path / "all.duckdb"

    _load_to_duckdb(f"file://{workbook_file}", database)

    assert _user_tables(database) == ["events", "inventory"]
    with duckdb.connect(str(database)) as connection:
        assert connection.execute("SELECT count(*) FROM landing.events").fetchone() == (
            2,
        )
        assert connection.execute(
            "SELECT count(*) FROM landing.inventory"
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT id, kind FROM landing.events ORDER BY id"
        ).fetchall() == [(1, "open"), (2, "closed")]


def test_named_worksheet_keeps_single_table_behavior(workbook_file, tmp_path):
    database = tmp_path / "one.duckdb"

    _load_to_duckdb(
        f"file://{workbook_file}#sheet_name=Events",
        database,
        dest_table="landing.selected_events",
    )

    assert _user_tables(database) == ["selected_events"]
    with duckdb.connect(str(database)) as connection:
        assert connection.execute(
            "SELECT id, kind FROM landing.selected_events ORDER BY id"
        ).fetchall() == [(1, "open"), (2, "closed")]


def test_json_list_selects_several_named_worksheets(workbook_file, tmp_path):
    database = tmp_path / "selected.duckdb"

    _load_to_duckdb(
        f'file://{workbook_file}#sheet_name=["Inventory","Events"]', database
    )

    assert _user_tables(database) == ["events", "inventory"]


def test_json_list_selects_several_worksheets_by_id(workbook_file, tmp_path):
    database = tmp_path / "selected-by-id.duckdb"

    _load_to_duckdb(f"file://{workbook_file}#sheet_id=[1,2]", database)

    assert _user_tables(database) == ["events", "inventory"]


@pytest.mark.parametrize("destination_scheme", ["csv", "file", "s3"])
def test_plural_workbook_rejects_single_table_destinations(
    workbook_file, tmp_path, destination_scheme
):
    destination = (
        f"{destination_scheme}://{tmp_path}/output.csv"
        if destination_scheme != "s3"
        else "s3://"
    )

    with pytest.raises(
        ValidationError,
        match=rf"'{destination_scheme}' destination cannot represent multiple worksheet tables",
    ):
        run_ingest(
            source_uri=f"file://{workbook_file}",
            dest_uri=destination,
            dest_table="landing.placeholder",
            progress="log",
        )


def test_single_worksheet_selector_still_supports_file_destination(
    workbook_file, tmp_path
):
    output = tmp_path / "events.csv"

    run_ingest(
        source_uri=f"file://{workbook_file}#sheet_name=Events",
        dest_uri=f"file://{output}",
        dest_table="landing.events",
        progress="log",
    )

    assert output.read_text().splitlines()[:3] == [
        "id,kind",
        "1,open",
        "2,closed",
    ]


def test_same_worksheet_name_across_glob_merges_rows(tmp_path):
    for name, row_id in (("a", 1), ("b", 2)):
        _write_workbook(
            tmp_path / f"{name}.xlsx",
            OrderedDict([("Events", [["id"], [row_id]])]),
        )
    database = tmp_path / "glob.duckdb"

    _load_to_duckdb(f"file://{tmp_path}/*.xlsx", database)

    assert _user_tables(database) == ["events"]
    with duckdb.connect(str(database)) as connection:
        assert connection.execute(
            "SELECT id FROM landing.events ORDER BY id"
        ).fetchall() == [(1,), (2,)]


def test_distinct_names_colliding_across_glob_name_both_files(tmp_path):
    first = _write_workbook(
        tmp_path / "first.xlsx", OrderedDict([("A B", [["id"], [1]])])
    )
    second = _write_workbook(
        tmp_path / "second.xlsx", OrderedDict([("A-B", [["id"], [2]])])
    )

    with pytest.raises(PipelineStepFailed) as error:
        _load_to_duckdb(f"file://{tmp_path}/*.xlsx", tmp_path / "collision.duckdb")

    message = str(error.value)
    assert "'A B'" in message
    assert "'A-B'" in message
    assert "'a_b'" in message
    assert first.as_uri() in message
    assert second.as_uri() in message


def test_distinct_names_colliding_across_filesystem_pages(tmp_path):
    first = _write_workbook(
        tmp_path / "000.xlsx", OrderedDict([("A B", [["id"], [0]])])
    )
    for index in range(1, 100):
        _write_workbook(
            tmp_path / f"{index:03}.xlsx",
            OrderedDict([("Events", [["id"], [index]])]),
        )
    second = _write_workbook(
        tmp_path / "100.xlsx", OrderedDict([("A-B", [["id"], [100]])])
    )

    with pytest.raises(PipelineStepFailed) as error:
        _load_to_duckdb(f"file://{tmp_path}/*.xlsx", tmp_path / "collision.duckdb")

    message = str(error.value)
    assert "'A B'" in message
    assert "'A-B'" in message
    assert first.as_uri() in message
    assert second.as_uri() in message


@pytest.mark.parametrize("suffix", ["xlsx", "ods"])
@pytest.mark.parametrize("empty_first", [True, False])
def test_empty_worksheet_does_not_reserve_normalized_name(
    tmp_path, suffix, empty_first
):
    sheets = [("A B", []), ("A-B", [["id"], [1]])]
    if not empty_first:
        sheets.reverse()
    workbook = _write_workbook(
        tmp_path / f"empty-collision.{suffix}", OrderedDict(sheets)
    )
    database = tmp_path / f"empty-collision-{suffix}.duckdb"

    _load_to_duckdb(f"file://{workbook}", database)

    assert _user_tables(database) == ["a_b"]
    with duckdb.connect(str(database)) as connection:
        assert connection.execute("SELECT id FROM landing.a_b").fetchall() == [(1,)]


def test_collision_within_one_workbook_fails_before_load(tmp_path):
    workbook = _write_workbook(
        tmp_path / "collision.xlsx",
        OrderedDict(
            [
                ("Quarterly Sales", [["id"], [1]]),
                ("quarterly-sales", [["id"], [2]]),
            ]
        ),
    )

    with pytest.raises(PipelineStepFailed, match="quarterly_sales"):
        _load_to_duckdb(f"file://{workbook}", tmp_path / "collision.duckdb")


@pytest.mark.parametrize(
    ("schema_naming", "expected"),
    [
        ("default", ["_ngstr_m_2026", "quarterly_sales"]),
        ("direct", ["Quarterly Sales", "Ångström 2026"]),
    ],
)
def test_worksheet_names_follow_active_naming_convention(
    tmp_path, schema_naming, expected
):
    workbook = _write_workbook(
        tmp_path / f"names-{schema_naming}.xlsx",
        OrderedDict(
            [
                ("Quarterly Sales", [["id"], [1]]),
                ("Ångström 2026", [["id"], [2]]),
            ]
        ),
    )
    database = tmp_path / f"names-{schema_naming}.duckdb"
    previous_naming = dlt.config.get("schema.naming")

    try:
        _load_to_duckdb(f"file://{workbook}", database, schema_naming=schema_naming)
        assert _user_tables(database) == expected
    finally:
        # ``dlt.config`` is process-global; do not let the direct-naming case
        # affect unrelated tests sharing this worker.
        dlt.config["schema.naming"] = previous_naming


def test_polars_calamine_preserves_numeric_values(workbook_file):
    import polars as pl

    reader = pl.read_excel if workbook_file.suffix == ".xlsx" else pl.read_ods

    assert reader(workbook_file.read_bytes(), sheet_name="Events").rows(named=True) == [
        {"id": 1, "kind": "open"},
        {"id": 2, "kind": "closed"},
    ]
