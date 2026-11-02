from omniload.core.factory import SourceDestinationFactory, parse_scheme_from_uri
from omniload.core.router import SqlSourceRouter


def test_scheme_is_parsed_from_uri_correctly():
    assert parse_scheme_from_uri("bigquery://my-project") == "bigquery"
    assert parse_scheme_from_uri("http://localhost:8080") == "http"
    assert parse_scheme_from_uri("file:///tmp/myfile") == "file"
    assert parse_scheme_from_uri("https://example.com?query=123") == "https"
    assert parse_scheme_from_uri("ftp://ftp.example.com/downloads/file.zip") == "ftp"
    assert (
        parse_scheme_from_uri("redshift+psycopg2://user:pw@host") == "redshift+psycopg2"
    )
    assert parse_scheme_from_uri("mysql+pymysql://user:pw@host") == "mysql+pymysql"


def test_exasol_schemes_resolve_to_the_sql_router():
    for uri in (
        "exa://sys:secret@host:8563",
        "exa+websocket://sys:secret@host:8563",
    ):
        factory = SourceDestinationFactory(uri, "duckdb:///out.duckdb")
        assert isinstance(factory.get_source(), SqlSourceRouter), uri
