import pytest
import sqlalchemy
from sqlalchemy.pool import NullPool

from tests.database.container import DESTINATIONS
from tests.util import invoke_ingest_command


@pytest.mark.skip("Currently inactive")
@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_github_to_database(dest):
    """
    # TODO(turtledev): use a token with higher rate limits
    # Integration testing when the access token is not provided, and it is only for the resource "repo_events
    """
    dest_uri = dest.start()
    source_uri = "github://?owner=panodata&repo=omniload"
    source_table = "repo_events"

    dest_table = "dest.github_repo_events"
    res = invoke_ingest_command(source_uri, source_table, dest_uri, dest_table)

    assert res.exit_code == 0
    dest_engine = sqlalchemy.create_engine(dest_uri, poolclass=NullPool)
    with dest_engine.connect() as dest_conn:
        res = dest_conn.exec_driver_sql(f"select count(*) from {dest_table}").fetchall()
    dest_engine.dispose()
    assert len(res) > 0
