"""The contract omniload offers for an `http://` or `https://` source URL.

Driven through the registered scheme (`run_ingest` and `SourceDestinationFactory`)
rather than a named class, so the matrix reads the same before and after the
connector behind that scheme is replaced. What it pins is what reaches the wire
and what lands in the destination, not one implementation of it.

Six server behaviours are covered because discovery and reading take different
code paths through them: a server that honours `Range`, one that ignores it, one
that answers chunked so no size is reported, one that renders an HTML directory
index, one that omits `Last-Modified`, and one that sends a malformed value. An
HTML index entry carries only its name and type, with neither a size nor a
modification time, even though a request for the concrete file returns both. See
`http_server.py`.

Every server is a local `http.server` thread: no Docker, no credentials, no
network.
"""

import ssl
from contextlib import contextmanager
from datetime import timedelta
from unittest.mock import patch

import duckdb
import pytest
from fsspec.implementations.memory import MemoryFileSystem

from dlt_filesystem.error import MissingConnectorOption
from dlt_filesystem.source.fsspec.http import (
    HttpFileSystem,
    HttpFilesystemSource,
    HttpModificationTimeError,
    HttpReadError,
)
from dlt_filesystem.source.lister import glob_files
from dlt_filesystem.source.model import FilesystemReference
from omniload import run_ingest
from tests.dlt_filesystem.http_server import (
    AUTH_PASSWORD_ENCODED,
    AUTH_USERNAME,
    EVENT_COUNT,
    HTTP_LAST_MODIFIED,
    PEOPLE,
    HttpFixture,
    closed_port,
)

#: The rows every document in the fixture root carries, in query order.
EXPECTED = [("Alice", 30), ("Bob", 41), ("Charlie", 25)]

#: A presigned-URL shape. `%2F` must survive to the wire byte for byte, because a
#: signature is computed over the encoded form; `%7E` normalizes to `~`, which is
#: what `requests` did and what every SigV4 canonicalization treats as equal.
SIGNED_QUERY = "X-Amz-Signature=abc%2Fdef%7Eghi&X-Amz-Expires=900"
SIGNED_QUERY_ON_THE_WIRE = "X-Amz-Signature=abc%2Fdef~ghi&X-Amz-Expires=900"


def load(
    server: HttpFixture,
    document: str,
    tmp_path,
    *,
    table: str = "",
    query: str = "",
    fragment: str = "",
    **options,
):
    """Ingest one document from the fixture server into a fresh duckdb file."""
    destination = tmp_path / "warehouse.duckdb"
    run_ingest(
        source_uri=server.url(document, query=query, fragment=fragment),
        dest_uri=f"duckdb:///{destination}",
        source_table=table,
        dest_table="out.people",
        progress="log",
        **options,
    )
    return destination


def records(source) -> list:
    """Flatten the data items a source yields into one list of rows."""
    collected: list = []
    for item in source:
        collected.extend(item) if isinstance(item, list) else collected.append(item)
    return collected


def rows(
    destination, statement: str = "select name, age from out.people order by name"
):
    connection = duckdb.connect(str(destination))
    try:
        return connection.sql(statement).fetchall()
    finally:
        connection.close()


DOCUMENTS = [
    pytest.param("people.csv", "", id="csv"),
    pytest.param("people.jsonl", "", id="jsonl"),
    pytest.param("people.json", "", id="json"),
    pytest.param("people.parquet", "", id="parquet"),
    pytest.param(
        "people-no-header.csv", "people-no-header.csv#csv_headless", id="csv_headless"
    ),
    pytest.param("feeds/events.csv", "", id="nested-path"),
]


@pytest.mark.parametrize(("document", "table"), DOCUMENTS)
def test_document_formats_load(range_server, tmp_path, document, table):
    """Every format the connector claims, read over plaintext `http://`.

    This is also the plaintext-`http` regression test: it proves the scheme by
    reading rows, not by listing files. dlt composes a file's URL through
    `dlt.common.storages.configuration.MAKE_URI_DISPATCH`, which registers
    `https` and not `http`, and the URL it composes for `http` is wrong in a way
    listing cannot see: it only fails when a reader opens the file.
    """
    destination = load(
        range_server,
        document,
        tmp_path,
        table=table,
        columns=["name:text,age:bigint"],
    )

    assert rows(destination) == EXPECTED


def test_signed_url_query_reaches_the_wire(range_server, tmp_path):
    """A signed URL loads, and its signature arrives at the server intact.

    Asserted from the fixture's own request log rather than from the row count:
    rows would also come back from a server that ignores the query, so only the
    recorded request can show the signature was neither dropped nor rewritten.
    """
    destination = load(range_server, "people.csv", tmp_path, query=SIGNED_QUERY)

    assert rows(destination) == EXPECTED
    assert range_server.queries(), "no request reached the server"
    assert set(range_server.queries()) == {SIGNED_QUERY_ON_THE_WIRE}


def test_server_that_ignores_range_loads_csv(no_range_server, tmp_path):
    """A server that answers every request with the whole body still loads."""
    destination = load(no_range_server, "people.csv", tmp_path)

    assert rows(destination) == EXPECTED


def test_server_that_ignores_range_loads_parquet(no_range_server, tmp_path):
    """The worst case for a range-less server: a reader that seeks.

    pyarrow reads a parquet footer before anything else, so this fails on a
    plain fsspec range read (fsspec raises rather than falling back) where the
    previous connector's whole-body download did not.
    """
    destination = load(no_range_server, "people.parquet", tmp_path)

    assert rows(destination) == EXPECTED


def test_chunked_response_without_content_length_loads_csv(chunked_server, tmp_path):
    """A chunked response reports no size, which must not be fatal.

    This is ordinary `Transfer-Encoding: chunked`, not an exotic case: the file
    is concrete and named, and the client simply cannot say how long it is.
    """
    destination = load(chunked_server, "people.csv", tmp_path)

    assert rows(destination) == EXPECTED


def test_chunked_response_without_content_length_loads_parquet(
    chunked_server, tmp_path
):
    """Unknown size plus a seeking reader: no size means no seekable file."""
    destination = load(chunked_server, "people.parquet", tmp_path)

    assert rows(destination) == EXPECTED


def test_percent_encoded_password_authenticates(auth_server, tmp_path):
    """Userinfo is percent-decoded before it is sent, as `requests` decoded it.

    The password here needs encoding to survive a URI at all (`@` would end the
    userinfo, `/` the netloc), so a connector that forwards the raw substring
    authenticates as the wrong user.
    """
    url = auth_server.url("people.csv")
    scheme, _, remainder = url.partition("://")
    credentialed = f"{scheme}://{AUTH_USERNAME}:{AUTH_PASSWORD_ENCODED}@{remainder}"

    destination = tmp_path / "warehouse.duckdb"
    run_ingest(
        source_uri=credentialed,
        dest_uri=f"duckdb:///{destination}",
        source_table="",
        dest_table="out.people",
        progress="log",
    )

    assert rows(destination) == EXPECTED
    assert [request.status for request in auth_server.requests].count(401) <= 2


def test_reader_reads_in_ranges(range_server, tmp_path):
    """A read is composed of range requests, not one download of the whole body.

    The shape the migration buys, and it needs pinning because rows come back
    either way. Listing probes the URL with one unranged `GET` before any reader
    opens it, and range support is established with a one-byte request, so those
    two are the only unranged reads the exchange may contain.
    """
    destination = tmp_path / "warehouse.duckdb"
    run_ingest(
        source_uri=range_server.url("events.jsonl"),
        dest_uri=f"duckdb:///{destination}",
        source_table="",
        dest_table="out.events",
        progress="log",
    )

    assert rows(destination, "select count(*) from out.events") == [(EVENT_COUNT,)]

    body_reads = [
        request
        for request in range_server.requests
        if request.method == "GET" and request.range_header != "bytes=0-0"
    ]
    assert len(body_reads) > 1, "no reader request was made at all"
    assert [request.is_ranged for request in body_reads] == [False] + [True] * (
        len(body_reads) - 1
    ), "the reader fell back to downloading the whole body"


def test_first_records_arrive_before_the_whole_body_is_served(range_server):
    """The first batch yields while most of the document is still on the server.

    Driven through the source rather than `run_ingest` because `block_size` is a
    connection argument, and an HTTP URL's query string is its address, so the
    only way to set one is programmatically. It has to be set at all: fsspec's
    default block is 5 MB, larger than any fixture here, and one block covering
    the whole file is indistinguishable from a download.

    Only a line-oriented reader can demonstrate this. pyarrow asks for a parquet
    file's entire data section in one read (measured: 20 row groups, one range
    covering all of them, and more bytes than the file when the block is small
    enough to make it re-read), so parquet is not a stream over any transport.
    """
    source = HttpFilesystemSource().dlt_source(
        range_server.url("events.jsonl"), "", block_size=16384
    )

    assert next(iter(source)), "the reader yielded nothing"

    body = range_server.body("events.jsonl")
    served = range_server.bytes_served(ranged_only=True)
    assert 0 < served < len(body) // 2, (
        f"first batch cost {served} of {len(body)} bytes"
    )


def test_missing_document_fails_loudly(range_server, tmp_path):
    """A URL that names nothing must raise, not load nothing and call it a success.

    Kept apart from the redaction case below on purpose: folding the two together
    lets a regression to a silent empty load satisfy that test's `xfail`, which is
    the louder of the two failures.
    """
    with pytest.raises(Exception) as exception:  # noqa: PT011 - tightened in the swap
        load(range_server, "absent.csv", tmp_path, query=SIGNED_QUERY)

    assert 404 in [request.status for request in range_server.requests]
    assert str(exception.value)


def test_missing_document_does_not_leak_the_query(range_server, tmp_path):
    """The message for an absent document must not carry the signature.

    Deliberately does not require an exception: whether one is raised at all is
    the previous test's contract, so this one reads whatever message came back
    (none, if nothing raised) and only judges what is in it.

    Redaction here is structural rather than scrubbed: the query is carried beside
    the file selection instead of inside it, so no string an error interpolates has
    ever held it. Neutering the family's own location-scrubbing does not make this
    test fail (measured), which is the point -- but it does mean the absence
    assertions need the positive one beside them to mean anything.
    """
    try:
        load(range_server, "absent.csv", tmp_path, query=SIGNED_QUERY)
    except Exception as error:  # noqa: BLE001 - the message is the subject
        message = str(error)
    else:
        message = ""

    assert "absent.csv" in message, "the failure does not name the file it looked for"
    assert "X-Amz-Signature" not in message
    assert "abc%2Fdef" not in message


# -- what the filesystem family adds that the previous connector did not have ---


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        pytest.param("people.csv.gz", EXPECTED, id="csv-gz"),
        pytest.param("people.json.gz", EXPECTED, id="json-gz"),
        pytest.param("people-pretty.json", EXPECTED, id="json-pretty"),
        pytest.param("people-object.json", EXPECTED[:1], id="json-object"),
    ],
)
def test_compressed_and_whole_document_formats_load(
    range_server, tmp_path, document, expected
):
    """Formats the shared reader stack brings with it.

    A gzipped document is the one case Polars cannot rescue on its own: the
    compression is read from the file name during listing and applied when the
    file is opened, which is family machinery the previous reader had no part in.
    """
    destination = load(range_server, document, tmp_path)

    assert rows(destination) == expected


def test_format_named_by_a_uri_fragment(range_server, tmp_path):
    """`#format` on the URI selects the reader, for a name that cannot say so."""
    destination = load(range_server, "people.dat", tmp_path, fragment="csv")

    assert rows(destination) == EXPECTED


def test_reader_hint_named_by_a_uri_fragment(range_server, tmp_path):
    """`#key=value` reaches the reader, so a dialect can be named per URL."""
    destination = load(
        range_server, "people-semicolon.csv", tmp_path, fragment="separator=;"
    )

    assert rows(destination) == EXPECTED


def test_https_loads_over_tls(tls_server, http_certificate):
    """`https://` end to end, against a certificate that is verified rather than skipped."""
    context = ssl.create_default_context(cafile=str(http_certificate[0]))
    source = HttpFilesystemSource().dlt_source(
        tls_server.url("people.csv"), "", ssl=context
    )

    assert records(source) == list(PEOPLE)
    assert tls_server.requests, "nothing reached the TLS server"


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        pytest.param("*.csv", ["alpha.csv", "bravo.csv"], id="flat"),
        pytest.param(
            "**/*.csv",
            ["alpha.csv", "bravo.csv", "sub/charlie.csv", "sub/deep/delta.csv"],
            id="recursive",
        ),
        pytest.param("sub/*.csv", ["sub/charlie.csv"], id="scoped"),
    ],
)
def test_html_directory_index_globs_exact_files(index_server, pattern, expected):
    """Wildcards follow the links exposed by a browsable directory index."""
    reference = build_reference(index_server.url(pattern))

    items = list(glob_files(reference.fs, reference.bucket_url, reference.file_glob))

    assert [item["relative_path"] for item in items] == expected
    assert [item["file_url"] for item in items] == [
        index_server.url(path) for path in expected
    ]
    assert all("size_in_bytes" not in item for item in items)


def test_globbed_and_concrete_modification_dates_match(index_server):
    """An index entry is enriched to the same header a concrete lookup sees."""
    globbed_reference = build_reference(index_server.url("*.csv"))
    globbed = list(
        glob_files(
            globbed_reference.fs,
            globbed_reference.bucket_url,
            globbed_reference.file_glob,
            filesystem_incremental=True,
        )
    )

    index_server.clear()
    concrete_reference = build_reference(index_server.url("alpha.csv"))
    concrete = list(
        glob_files(
            concrete_reference.fs,
            concrete_reference.bucket_url,
            concrete_reference.file_glob,
            filesystem_incremental=True,
        )
    )

    expected = HTTP_LAST_MODIFIED.replace(microsecond=0)
    assert globbed[0]["modification_date"] == expected
    assert concrete[0]["modification_date"] == expected


def test_plain_index_glob_issues_no_per_file_metadata_requests(index_server):
    reference = build_reference(index_server.url("*.csv"))

    list(glob_files(reference.fs, reference.bucket_url, reference.file_glob))

    file_heads = [
        request
        for request in index_server.requests
        if request.method == "HEAD" and request.path.endswith(".csv")
    ]
    assert file_heads == []


def test_incremental_index_glob_issues_one_head_per_selected_file(index_server):
    reference = build_reference(index_server.url("*.csv"))

    items = list(
        glob_files(
            reference.fs,
            reference.bucket_url,
            reference.file_glob,
            filesystem_incremental=True,
        )
    )

    assert len(items) == 2
    file_heads = [
        request
        for request in index_server.requests
        if request.method == "HEAD" and request.path.endswith(".csv")
    ]
    assert len(file_heads) == 2


def test_concrete_incremental_selection_issues_no_second_head(index_server):
    reference = build_reference(index_server.url("alpha.csv"))

    items = list(
        glob_files(
            reference.fs,
            reference.bucket_url,
            reference.file_glob,
            filesystem_incremental=True,
        )
    )

    assert len(items) == 1
    assert [request.method for request in index_server.requests].count("HEAD") == 1


def test_modified_reattaches_query_but_keeps_it_out_of_missing_header_error(
    no_last_modified_server,
):
    filesystem = HttpFileSystem(url_query=SIGNED_QUERY)
    url = no_last_modified_server.url("people.csv")

    with pytest.raises(HttpModificationTimeError) as exception:
        filesystem.modified(url)

    message = str(exception.value)
    assert url in message
    assert "Last-Modified" in message
    assert "X-Amz-Signature" not in message
    assert [request.method for request in no_last_modified_server.requests] == ["HEAD"]
    assert no_last_modified_server.queries() == [SIGNED_QUERY_ON_THE_WIRE]


def test_modified_rejects_a_malformed_header(malformed_last_modified_server):
    filesystem = HttpFileSystem()
    url = malformed_last_modified_server.url("people.csv")

    with pytest.raises(HttpModificationTimeError) as exception:
        filesystem.modified(url)

    message = str(exception.value)
    assert url in message
    assert "malformed 'Last-Modified'" in message
    assert "not-an-http-date" in message


# -- guardrails: what must never happen to a query, a credential, or an identity -


def build_reference(uri: str, table: str = "", **kwargs) -> FilesystemReference:
    """Capture the reference a source builds, without reading a byte."""
    captured: dict = {}

    def fake_reader(ref: FilesystemReference):
        captured["ref"] = ref
        return "SENTINEL"

    with patch("dlt_filesystem.source.core.resource_for_reader", fake_reader):
        assert HttpFilesystemSource().dlt_source(uri, table, **kwargs) == "SENTINEL"

    return captured["ref"]


@contextmanager
def constructor_spy():
    """Record the keywords the filesystem class is constructed with.

    `cachable` is off because fsspec keys its instance cache on the constructor
    arguments and skips `__init__` on a hit, so a cachable spy records nothing for
    the second equal construction and the assertion would pass vacuously.
    """
    calls: list[dict] = []

    class SpyFileSystem(MemoryFileSystem):
        cachable = False
        protocol = "http"

        def __init__(self, *args, **kwargs):
            calls.append(dict(kwargs))
            super().__init__()

    with patch.object(
        HttpFilesystemSource, "fs_class", property(lambda self: SpyFileSystem)
    ):
        yield calls


def test_no_query_parameter_reaches_the_filesystem_constructor(range_server):
    """The query is an address, so it is carried, never spread into arguments.

    A signature that arrived as a constructor keyword would be forwarded into
    aiohttp as a request argument and fragment fsspec's instance cache, one entry
    per signature.
    """
    with constructor_spy() as calls:
        build_reference(range_server.url("people.csv", query=SIGNED_QUERY))

    assert calls, "the filesystem was never constructed"
    received = calls[-1]
    assert set(received) == {"url_query", "client_kwargs"}
    assert received["url_query"] == SIGNED_QUERY


def test_credentials_do_not_reach_the_bucket_url_or_the_identity(auth_server):
    """A URL's userinfo becomes one request header and appears nowhere else."""
    url = auth_server.url("people.csv")
    scheme, _, remainder = url.partition("://")
    credentialed = f"{scheme}://{AUTH_USERNAME}:{AUTH_PASSWORD_ENCODED}@{remainder}"

    with constructor_spy() as calls:
        reference = build_reference(credentialed)

    received = calls[-1]
    assert "username" not in received
    assert "password" not in received
    authorization = received["client_kwargs"]["headers"]["Authorization"]
    assert authorization.startswith("Basic ")
    assert AUTH_USERNAME not in reference.bucket_url
    assert AUTH_PASSWORD_ENCODED not in reference.bucket_url
    assert AUTH_USERNAME not in reference.incremental_resource_name


def test_identity_is_derived_from_the_query_free_url(range_server):
    """Rotating a signature must not re-key the data or the incremental state."""
    first = build_reference(range_server.url("people.csv", query="X-Amz-Signature=one"))
    second = build_reference(
        range_server.url("people.csv", query="X-Amz-Signature=two")
    )

    assert first.file_glob == second.file_glob == "people.csv"
    assert first.bucket_url == second.bucket_url
    assert first.incremental_resource_name == second.incremental_resource_name
    assert "X-Amz-Signature" not in first.file_glob + first.bucket_url


def test_listed_file_url_is_the_query_free_url(range_server):
    """The record's primary key is `file_url`, so the query must not be in it.

    Also the plaintext-`http` composition, at the level where it is wrong: a URL
    built through dlt's fallback would read `http://http://host/...` here.
    """
    reference = build_reference(range_server.url("people.csv", query=SIGNED_QUERY))

    items = list(glob_files(reference.fs, reference.bucket_url, reference.file_glob))

    assert [item["file_url"] for item in items] == [range_server.url("people.csv")]
    assert items[0]["size_in_bytes"] == len(range_server.body("people.csv"))


def test_listed_file_survives_a_server_that_reports_no_size(chunked_server):
    """Discovery must not crash on a response that cannot state a length.

    The size is reported as absent rather than as zero, which would be
    indistinguishable from an empty file.
    """
    reference = build_reference(chunked_server.url("people.csv"))

    items = list(glob_files(reference.fs, reference.bucket_url, reference.file_glob))

    assert [item["file_name"] for item in items] == ["people.csv"]
    assert "size_in_bytes" not in items[0]


def test_filesystem_instances_are_not_retained_between_signatures(range_server):
    """A credential must not outlive the run in a process-global cache.

    fsspec keys its instance cache on the constructor arguments and keeps every
    instance for the life of the process, so a caching filesystem would hold one
    entry per signature, each retaining that signature and any auth header.
    """
    HttpFileSystem.clear_instance_cache()

    build_reference(range_server.url("people.csv", query="X-Amz-Signature=one"))
    build_reference(range_server.url("people.csv", query="X-Amz-Signature=two"))

    assert HttpFileSystem.cachable is False
    assert HttpFileSystem._cache == {}


def test_a_failed_whole_body_read_does_not_leak_the_query(no_range_server):
    """The likeliest failure a signed URL has, on the path that reads it whole.

    fsspec answers a `404` with the query-free path it was given, but hands every
    other status to aiohttp, whose error names the URL it requested. This is the
    read the source performs itself, so it is the one it can report.
    """
    filesystem = HttpFileSystem(url_query=SIGNED_QUERY)

    with pytest.raises(HttpReadError) as exception:
        filesystem.open(no_range_server.url("forbidden/data.csv"))

    message = str(exception.value)
    assert "403" in message
    assert "forbidden/data.csv" in message
    assert "X-Amz-Signature" not in message
    assert "abc%2Fdef" not in message
    assert any(
        request.query == SIGNED_QUERY_ON_THE_WIRE
        for request in no_range_server.requests
    ), "the signature never reached the server, so the test proves nothing"


def test_filesystem_incremental_is_supported_by_run_and_dry_run(range_server, tmp_path):
    destination = load(
        range_server, "people.csv", tmp_path, filesystem_incremental=True
    )

    assert rows(destination) == EXPECTED
    assert HttpFilesystemSource().supports_filesystem_incremental() is True
    load(
        range_server,
        "people.csv",
        tmp_path,
        filesystem_incremental=True,
        dry_run=True,
    )


def test_filesystem_incremental_is_supported_by_the_source_itself(range_server):
    source = HttpFilesystemSource().dlt_source(
        range_server.url("people.csv"), "", filesystem_incremental=True
    )

    assert records(source) == list(PEOPLE)


def test_missing_last_modified_refuses_incremental_but_not_plain_load(
    no_last_modified_server, tmp_path
):
    url = no_last_modified_server.url("people.csv")

    with pytest.raises(Exception) as exception:  # noqa: PT011 - dlt wraps extraction
        load(
            no_last_modified_server,
            "people.csv",
            tmp_path,
            filesystem_incremental=True,
        )

    message = str(exception.value)
    assert url in message
    assert "Last-Modified" in message

    no_last_modified_server.clear()
    destination = load(no_last_modified_server, "people.csv", tmp_path)
    assert rows(destination) == EXPECTED


def test_incremental_html_index_loads_unchanged_files_once_then_one_modified_file(
    index_server, tmp_path
):
    destination = tmp_path / "incremental-index.duckdb"
    pipelines_dir = tmp_path / "state"

    def run(query: str = "") -> None:
        run_ingest(
            source_uri=index_server.url("**/*.csv", query=query),
            dest_uri=f"duckdb:///{destination}",
            source_table="",
            dest_table="out.index_values",
            pipelines_dir=str(pipelines_dir),
            filesystem_incremental=True,
            progress="log",
        )

    try:
        run()
        run()
        assert rows(
            destination,
            "select value, count(*) from out.index_values group by value order by value",
        ) == [("alpha", 1), ("bravo", 1), ("charlie", 1), ("delta", 1)]

        index_server.set_modified(
            "alpha.csv", HTTP_LAST_MODIFIED + timedelta(minutes=1)
        )
        run()

        assert rows(
            destination,
            "select value, count(*) from out.index_values group by value order by value",
        ) == [("alpha", 2), ("bravo", 1), ("charlie", 1), ("delta", 1)]
    finally:
        index_server.set_modified("alpha.csv", HTTP_LAST_MODIFIED)


def test_incremental_cursor_identity_survives_signed_query_rotation(
    index_server, tmp_path
):
    destination = tmp_path / "signed-incremental.duckdb"
    pipelines_dir = tmp_path / "signed-state"

    for signature in ("one", "two"):
        run_ingest(
            source_uri=index_server.url("*.csv", query=f"X-Amz-Signature={signature}"),
            dest_uri=f"duckdb:///{destination}",
            source_table="",
            dest_table="out.signed_values",
            pipelines_dir=str(pipelines_dir),
            filesystem_incremental=True,
            progress="log",
        )

    assert rows(
        destination,
        "select value, count(*) from out.signed_values group by value order by value",
    ) == [("alpha", 1), ("bravo", 1)]


def test_incremental_key_is_refused_through_a_run(range_server, tmp_path):
    """The only path that still rejects it: the check is centralized in
    `omniload.api` (Phase 3 of GH-316's extraction prep), keyed on the source's
    declared `consumed_run_options()` rather than on a per-source guard reading
    `incremental_key` or `requested_incremental_key` out of `**kwargs`. A direct
    `HttpFilesystemSource().dlt_source(..., incremental_key=...)` call no longer
    raises: the name is not part of the declared signature, so it lands in
    `**kwargs` and merges into the fsspec constructor untouched, which is the
    same fate any other undeclared run option now has (see
    `tests/dlt_filesystem/test_source_option_ownership.py`).
    """
    with pytest.raises(ValueError, match="should not provide incremental_key"):
        load(range_server, "people.csv", tmp_path, incremental_key="modified_at")


def test_file_format_argument_names_the_reader(range_server):
    """A programmatic `file_format=` is a reader choice, not a connection argument."""
    reference = build_reference(range_server.url("people.dat"), file_format="csv")

    assert reference.reader_name == "read_csv"


def test_chunksize_argument_reaches_the_reader_not_the_filesystem(range_server):
    """The same for `chunksize=`, which aiohttp would reject as a request keyword."""
    with constructor_spy() as calls:
        reference = build_reference(range_server.url("people.csv"), chunksize=7)

    assert reference.hints == {"chunksize": 7}
    assert set(calls[-1]) == {"url_query", "client_kwargs"}


def test_missing_host_is_reported(range_server):
    with pytest.raises(MissingConnectorOption, match="host is required"):
        HttpFilesystemSource().dlt_source("http://", "")


def test_percent_encoded_document_name_reaches_the_wire_unchanged(
    range_server, tmp_path
):
    """A name that only survives a URL encoded must not be encoded a second time.

    `caf%C3%A9.csv` is already correct; escaping its `%` again asks the server for a
    file whose name literally contains `%C3%A9`, which is a 404.
    """
    destination = load(range_server, "caf%C3%A9.csv", tmp_path)

    assert rows(destination) == EXPECTED
    assert {request.path for request in range_server.requests} == {"/caf%C3%A9.csv"}


def test_a_url_that_carries_its_own_query_keeps_it(range_server):
    """The source query is re-attached, never appended to a query already there."""
    filesystem = HttpFileSystem(url_query="X-Amz-Signature=source")

    encoded = str(filesystem.encode_url(range_server.url("people.csv", query="own=1")))

    assert encoded.endswith("?own=1")
    assert "X-Amz-Signature" not in encoded


def test_block_size_zero_streams_without_probing(range_server):
    """An explicit ask for the streaming interface is honoured as asked.

    No range probe is issued, because nothing about range support would change the
    answer: the caller has already said it wants a stream.
    """
    filesystem = HttpFileSystem()

    with filesystem.open(range_server.url("people.csv"), block_size=0) as file:
        assert file.read() == range_server.body("people.csv")

    assert [request.range_header for request in range_server.ranged()] == []


def test_probe_answers_no_ranges_when_the_server_cannot_be_reached(range_server):
    """A probe that cannot connect answers "no ranges" instead of raising.

    The failure is not swallowed: the read that follows reports it, and reports it
    without the query, which is what raising from the probe would have leaked.
    """
    filesystem = HttpFileSystem(url_query=SIGNED_QUERY)
    unreachable = f"http://127.0.0.1:{closed_port()}/people.csv"

    assert filesystem._range_size(unreachable) is None

    with pytest.raises(OSError) as exception:
        filesystem.open(unreachable)
    assert "X-Amz-Signature" not in str(exception.value)
