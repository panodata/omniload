"""Pin the ownership boundary between this package's own options and connectors.

`FilesystemSource.consumed_run_options()` (`source/base.py`) names the two run
options this family understands: `filesystem_incremental` and `column_types`.
Every `dlt_source` implementation declares them by name in its own signature,
never reads them out of `**kwargs`, so the property this file pins is now a
consequence of the signatures rather than of a runtime split: what remains in
`**kwargs` is connector-only, by construction, and merges into the fsspec/Arrow
constructor untouched.

What `omniload.api` sends a filesystem source (only the two names it declares,
everything else filtered out before the call) is a separate property, tested in
`tests/main/test_filesystem_option_dispatch.py`: this file drives `dlt_source`
directly, never through `run_ingest`.

The spy needs a per-connector patch map because there is no single construction
hook: most connectors resolve a `fs_class` property, FTP imports its class from
the fsspec module, SFTP goes through `fsspec.filesystem`, and the local and Azure
sources wrap a pyarrow filesystem, whose constructor is recorded rather than
replaced.
"""

import ast
import json
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from fsspec.implementations.memory import MemoryFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.fsspec.ftp import FTPSource
from dlt_filesystem.source.fsspec.local import LocalFilesystemSource
from dlt_filesystem.source.impl.remote import (
    AzureSource,
    S3CompatibleSource,
    SFTPSource,
)
from dlt_filesystem.source.model import FilesystemReference
from omniload.api import RUN_OPTION_KEYS
from omniload.core.factory import SourceDestinationFactory

ASSETS = Path(__file__).resolve().parents[1] / "assets"
PRIVATE_KEY_FILE = (ASSETS / "privatekey.pem").as_posix()
PRIVATE_KEY_FINGERPRINT = (ASSETS / "privatekey-fingerprint.txt").read_text().strip()
OCI_CONFIG = json.dumps(
    {
        "user": "ocid1.user.oc1..24g4uzg",
        "region": "us-ashburn-1",
        "tenancy": "ocid1.tenancy.oc1..23423r3",
        "key_file": PRIVATE_KEY_FILE,
        "fingerprint": PRIVATE_KEY_FINGERPRINT,
    },
    # Compact, because the value rides in a query string and a space would end it.
    separators=(",", ":"),
)
GRAPH_AUTH = (
    "client_id=1d2befad-2f22-4124-a779-b147dfeca342"
    "&tenant_id=6b337423-f504-4060-a91b-e9eaaf782609"
    "&client_secret=abc~xyz789EXAMPLE_foo"
)

#: The three names every source in this family declares by name and consumes as
#: a resource option, never as a connector keyword. Values chosen to be provably
#: present on the reference if forwarded correctly, and provably absent from a
#: constructor spy if they leak. None of the `CASES` URIs below carry a `#`
#: fragment, so `reader_hints`' sentinel key cannot collide with a URI hint.
DECLARED_OPTIONS: dict[str, Any] = {
    "filesystem_incremental": True,
    "column_types": {"name": {"data_type": "text"}},
    "reader_hints": {"probe_hint": "from-run"},
}


@dataclass
class Case:
    """One registered filesystem-family scheme and a URI that reaches its constructor."""

    scheme: str
    uri: str
    table: str = ""
    #: Connection arguments this URI carries, which must survive the split.
    expect_kwargs: tuple[str, ...] = field(default_factory=tuple)


#: One case per filesystem-family scheme in `omniload.core.registry`.
CASES = [
    Case(
        "abfss",
        "abfss://schrott@acme.dfs.core.windows.net/path/to/data.parquet?account_name=acme&account_key=secret",
        expect_kwargs=("account_name", "account_key"),
    ),
    Case(
        "adls",
        "adls://schrott@acme.dfs.core.windows.net/path/to/data.parquet?account_name=acme&account_key=secret",
        expect_kwargs=("account_name", "account_key"),
    ),
    Case(
        "az",
        "az://schrott@acme.dfs.core.windows.net/path/to/data.parquet?account_name=acme&account_key=secret",
        expect_kwargs=("account_name", "account_key"),
    ),
    Case("dbfs", "dbfs:/Volumes/catalog/schema/volume/path/to/data.parquet"),
    Case(
        "dropbox",
        "dropbox://path/to/data.parquet?token=secret",
        expect_kwargs=("token",),
    ),
    Case("file", "file://__TMP__/data.parquet"),
    Case(
        "ftp",
        "ftp://username:password@intranet.example.org/path/to/data.parquet?tls=tls",
        expect_kwargs=("host", "username", "password"),
    ),
    Case(
        "gdrive",
        "gdrive://path/to/data.parquet?token=anon",
        expect_kwargs=("token",),
    ),
    Case(
        "gs",
        "gs://table-bucket-name/path/to/data.parquet?credentials_path=/path/to/service-account.json",
        expect_kwargs=("token",),
    ),
    Case(
        "hdfs",
        "hdfs://example.com:8020/path/to/data.parquet?user=test",
        expect_kwargs=("host", "port", "user"),
    ),
    Case(
        # An HTTP URL's query is part of its address: it is carried whole rather
        # than spread into connector options, which is what `url_query` is.
        "http",
        "http://public.example.org/path/to/data.parquet?X-Amz-Signature=abc%2Fdef",
        expect_kwargs=("url_query", "client_kwargs"),
    ),
    Case(
        # WebDAV takes its URL positionally and folds any credentials into `auth`.
        "http+webdav",
        "http+webdav://public.example.org/path/to/data.parquet",
        expect_kwargs=("auth",),
    ),
    Case(
        "https",
        "https://public.example.org/path/to/data.parquet",
        expect_kwargs=("url_query", "client_kwargs"),
    ),
    Case(
        "https+webdav",
        "https+webdav://username:password@cloud.example.org:4443/remote.php/webdav",
        table="path/to/data.parquet",
        expect_kwargs=("auth",),
    ),
    Case(
        "msgd",
        f"msgd://site_name/drive_name/path/to/data.parquet?{GRAPH_AUTH}",
        expect_kwargs=("client_id", "tenant_id", "client_secret"),
    ),
    Case(
        "oci",
        f"oci://bucket@namespace/prefix/path/to/data.parquet?iam_type=api_key&config={OCI_CONFIG}",
        expect_kwargs=("config",),
    ),
    Case(
        "onedrive",
        f"onedrive://drive_name/path/to/data.parquet?{GRAPH_AUTH}",
        expect_kwargs=("client_id", "tenant_id", "client_secret"),
    ),
    Case(
        "oss",
        "oss://bucket/path/to/data.parquet?endpoint=http://oss-cn-hangzhou.aliyuncs.com/&key=foo&secret=bar",
        expect_kwargs=("endpoint", "key", "secret"),
    ),
    Case(
        "r2",
        "r2://bucket/path/to/data.parquet?access_key_id=foo&secret_access_key=bar",
        expect_kwargs=("access_key", "secret_key"),
    ),
    Case(
        "s3",
        "s3://bucket/path/to/data.parquet?access_key_id=foo&secret_access_key=bar",
        expect_kwargs=("access_key", "secret_key"),
    ),
    Case(
        "sftp",
        "sftp://username:password@intranet.example.org:2222/path/to/data.parquet",
        expect_kwargs=("host", "port", "username", "password"),
    ),
    Case(
        "sharepoint",
        f"sharepoint://site_name/drive_name/path/to/data.parquet?{GRAPH_AUTH}",
        expect_kwargs=("client_id", "tenant_id", "client_secret"),
    ),
    Case(
        "smb",
        "smb://workgroup;user:password@server.example.org:445/path/to/data.parquet",
        expect_kwargs=("host", "port", "username", "password"),
    ),
    Case(
        "webhdfs",
        "webhdfs://host:9870/endpoint",
        table="path/to/data.parquet",
        expect_kwargs=("host", "port"),
    ),
]


def _skip_unsupported(scheme: str) -> None:
    """Mirror the platform limits the touch matrix already documents."""
    if scheme in {"dbfs", "hdfs"} and sys.version_info < (3, 11):
        pytest.skip(f"{scheme}:// needs Python 3.11+")
    if scheme == "oci" and sys.platform == "win32":
        pytest.skip("oci:// fails testing on Windows")


def _spy_class(calls: list[dict[str, Any]], protocol: str):
    """Build a filesystem class that records the keywords it was constructed with.

    `cachable` is off because fsspec keys its instance cache on the constructor
    arguments and skips `__init__` on a hit, which would silently record nothing
    for the second case that happens to construct an equal filesystem.
    """

    class SpyFileSystem(MemoryFileSystem):
        cachable = False
        # `ArrowFSWrapper` reads `type_name` from the native filesystem it wraps.
        type_name = protocol

        def __init__(self, *args, **kwargs):
            calls.append(dict(kwargs))
            super().__init__()

    SpyFileSystem.protocol = protocol
    return SpyFileSystem


@contextmanager
def _spy_on_filesystem(source, scheme: str):
    """Patch whatever construction hook this connector actually uses."""
    calls: list[dict[str, Any]] = []
    native_protocol = "s3" if isinstance(source, S3CompatibleSource) else scheme
    spy = _spy_class(calls, native_protocol)

    if isinstance(source, FTPSource):
        # Imported into the function body from the fsspec module, not via `fs_class`.
        with mock.patch("fsspec.implementations.ftp.FTPFileSystem", spy):
            yield calls
    elif isinstance(source, SFTPSource):
        with mock.patch("fsspec.filesystem", lambda _protocol, **kwargs: spy(**kwargs)):
            yield calls
    elif isinstance(source, (AzureSource, LocalFilesystemSource)):
        # Wraps a pyarrow filesystem, so record that constructor instead of replacing
        # it: `ArrowFSWrapper` needs a real arrow filesystem underneath. Recording it
        # rather than asserting "no spy fired" is what makes the empty-kwargs claim
        # falsifiable, so forwarding a run option here would fail the same assertion
        # as everywhere else. It also pins the keywords against Arrow itself, which
        # rejects an unknown one where a stand-in would have accepted it.
        import pyarrow.fs

        native_name = (
            "AzureFileSystem" if isinstance(source, AzureSource) else "LocalFileSystem"
        )
        real_class = getattr(pyarrow.fs, native_name)

        def recording_native(*args, **kwargs):
            calls.append(dict(kwargs))
            return real_class(*args, **kwargs)

        with mock.patch.object(pyarrow.fs, native_name, recording_native):
            yield calls
    else:
        with mock.patch.object(type(source), "fs_class", property(lambda self: spy)):
            yield calls


def _drive(case: Case, tmp_path, **overrides) -> tuple[FilesystemReference, list[dict]]:
    """Build the source for one scheme and capture both sides of the boundary.

    Only ever passes the two names this family declares (`DECLARED_OPTIONS`), plus
    whatever `overrides` names deliberately: `dlt_source` is driven directly here,
    never through `omniload.api`, so this exercises the package's own contract, not
    the dispatch that narrows what a filesystem source receives from a run.
    """
    # Substituted by hand rather than with `str.format`, because the OCI URI carries a
    # JSON object in its query string and its braces are not format fields.
    uri = case.uri.replace("__TMP__", tmp_path.as_posix())
    source = SourceDestinationFactory(uri, "file://").get_source()
    options = {**DECLARED_OPTIONS, **overrides}

    with (
        mock.patch("dlt_filesystem.source.core.resource_for_reader") as build,
        _spy_on_filesystem(source, case.scheme) as calls,
    ):
        source.dlt_source(uri=uri, table=case.table, **options)

    assert build.call_count == 1, f"{case.scheme}: reference was not built once"
    return build.call_args.args[0], calls


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.scheme)
def test_declared_options_never_reach_the_constructor(case, tmp_path):
    """The headline package contract: the two names this family owns stay off the wire."""
    _skip_unsupported(case.scheme)
    _, calls = _drive(case, tmp_path)

    assert calls, f"{case.scheme}: the filesystem spy was never constructed"
    for received in calls:
        leaked = sorted(set(received) & set(DECLARED_OPTIONS))
        assert leaked == [], f"{case.scheme}: declared options reached the constructor"


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.scheme)
def test_uri_connection_options_survive_the_split(case, tmp_path):
    """The guardrail: narrowing the package's own vocabulary must not drop real kwargs."""
    _skip_unsupported(case.scheme)
    _, calls = _drive(case, tmp_path)

    received = calls[-1]
    missing = [key for key in case.expect_kwargs if key not in received]
    assert missing == [], f"{case.scheme}: connection arguments were dropped"
    if not case.expect_kwargs:
        # This URI addresses its object by path alone, so a clean constructor is the
        # whole assertion: anything present would have come from the declared options.
        assert received == {}


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.scheme)
def test_resource_options_reach_the_reference(case, tmp_path):
    """All three options this family owns land on the reference, on every scheme."""
    _skip_unsupported(case.scheme)
    reference, _ = _drive(case, tmp_path)

    assert reference.filesystem_incremental is True
    assert reference.column_types == DECLARED_OPTIONS["column_types"]
    assert reference.hints.get("probe_hint") == "from-run"


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.scheme)
def test_lister_is_namespaced_only_when_incremental_is_requested(case, tmp_path):
    """The no-op fingerprint: a plain `filesystem` lister means the flag was dropped."""
    _skip_unsupported(case.scheme)

    enabled, _ = _drive(case, tmp_path, filesystem_incremental=True)
    disabled, _ = _drive(case, tmp_path, filesystem_incremental=False)

    assert enabled.filesystem_incremental is True
    assert disabled.filesystem_incremental is False
    assert enabled.incremental_resource_name.startswith("filesystem_")
    assert enabled.incremental_resource_name != "filesystem"


def test_uri_column_types_remains_the_fallback(tmp_path):
    """A URI-supplied value still reaches the reference when the run supplies none."""
    case = Case("ftp", "ftp://user:pw@host/bucket/data.csv?column_types=from-uri")
    reference, calls = _drive(case, tmp_path, column_types=None)

    assert reference.column_types == "from-uri"
    assert "column_types" not in calls[-1]


def test_run_column_types_wins_over_the_uri(tmp_path):
    """Where both name it, the run value is the one the reader gets."""
    case = Case("ftp", "ftp://user:pw@host/bucket/data.csv?column_types=from-uri")
    reference, _ = _drive(case, tmp_path)

    assert reference.column_types == DECLARED_OPTIONS["column_types"]


def test_uri_filesystem_incremental_never_reaches_the_constructor(tmp_path):
    """The query string is the second carrier, and it gets the same rule as `**kwargs`.

    `filesystem_incremental` is the package's own name, so it is stripped whichever
    carrier it rode in on. A genuine connection argument beside it is unaffected.
    """
    case = Case(
        "ftp",
        "ftp://user:pw@host/bucket/data.csv?filesystem_incremental=true&tls=true",
    )
    _, calls = _drive(case, tmp_path)

    received = calls[-1]
    assert "filesystem_incremental" not in received
    assert received["tls"] is True


def test_a_name_outside_the_package_vocabulary_now_reaches_the_constructor(tmp_path):
    """The named behaviour change: an omniload run option this package does not
    declare is no longer this package's business, so it is no longer stripped here.

    Before this inversion, every name in omniload's fifteen-name run vocabulary was
    stripped inside the package (`RUN_OPTION_KEYS`), whichever carrier it arrived
    on. After it, only the two names this family declares (`filesystem_incremental`,
    `column_types`) are the package's concern; a run option it does not know about,
    like `page_size`, is omniload's to filter before the call, not this package's.
    Called directly (as this file always does), nothing filters it, so it survives
    to the constructor -- confirmed harmless here because the spy accepts anything,
    but not harmless on every backend (see the WebDAV case below).
    """
    case = Case(
        "ftp",
        "ftp://user:pw@host/bucket/data.csv?page_size=7&tls=true",
    )
    _, calls = _drive(case, tmp_path)

    received = calls[-1]
    assert received["page_size"] == "7"
    assert received["tls"] is True


def test_direct_connector_kwarg_reaches_the_constructor(tmp_path):
    """The subtractive property, on the `**kwargs` carrier rather than the URI.

    Every other test in this matrix supplies connector arguments through the
    URI query string (`case.expect_kwargs`). A caller can also pass one
    directly to `dlt_source` (a library caller, or `--filesystem-hint`-style
    programmatic use), and it has to reach the constructor the same way: it is
    not one of the three declared names, so it is connector-only by
    construction (an explicit keyword-only parameter, not a subtraction from
    `**kwargs`). Also checks precedence: a directly-passed value wins over the
    same name arriving on the URI, since `fs_kwargs.update(kwargs)` runs after
    the URI-derived base.
    """
    case = Case("ftp", "ftp://user:pw@host/bucket/data.csv?block_size=100")
    _, calls = _drive(case, tmp_path, block_size=999)

    received = calls[-1]
    assert received["block_size"] == 999


def test_webdav_rejects_an_omniload_run_option_it_does_not_declare(tmp_path):
    """The confirmed regression from the inversion, on a real constructor.

    `webdav4.fsspec.WebdavFileSystem` (unlike the FTP spy above) actually validates
    its keyword arguments, so `page_size` -- a name from omniload's own run
    vocabulary that this package does not declare -- now raises where it used to be
    silently dropped. This is the shape Open Question 2 in
    `PLAN_ISSUE_316_EXTRACTION_PREP.md` names: some backends stay silent (FTP,
    above), at least one raises. Named here as the one to point to.
    """
    from dlt_filesystem.source.fsspec.webdav import WebdavSource

    source = WebdavSource()
    with pytest.raises(TypeError, match="page_size"):
        source.dlt_source(
            uri="http+webdav://public.example.org/path/to/data.parquet?page_size=7",
            table="",
        )


def _api_call_site_keywords() -> set[str]:
    """Read the run-option names `run_ingest` builds for `source.dlt_source`.

    The call site expands a filtered dict (`**run_options`) rather than listing
    keywords one by one, so the names live in the dict literal assigned to
    `run_options`, not in the call's own keyword list.
    """
    import omniload.api

    source_file = omniload.api.__file__
    assert source_file is not None
    tree = ast.parse(Path(source_file).read_text())
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "run_options"
            and isinstance(node.value, ast.Dict)
        ):
            return {
                key.value
                for key in node.value.keys
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
    raise AssertionError("no `run_options` dict literal found in omniload.api")


def test_run_option_keys_match_the_api_call_site():
    """Adding a run parameter without classifying it must fail here, not leak silently."""
    assert _api_call_site_keywords() == set(RUN_OPTION_KEYS)


def test_every_registered_filesystem_scheme_is_covered():
    """A new transport cannot join the family without landing in this matrix."""
    from omniload.core.registry import sources

    # Read the unresolved dotted paths, so asserting on the registry does not import
    # every SaaS connector just to learn which module a scheme belongs to.
    registered = {
        scheme
        for scheme, dotted_path in sources._paths.items()
        if dotted_path.startswith("dlt_filesystem.")
    }
    assert registered == {case.scheme for case in CASES}


def test_reference_defaults_read_as_a_run_that_enabled_nothing():
    """`infer_resource` may be called without options; that must not enable anything."""
    from dlt_filesystem.source.model import ResourceOptions

    options = ResourceOptions()
    assert options.filesystem_incremental is False
    assert options.column_types is None


def test_declared_options_never_appear_in_the_package_as_omniload_names():
    """Grep-based guardrail: the 13 omniload owns and this package does not must
    never appear as an identifier under `src/dlt_filesystem/`.

    Sourced from both sides so it cannot drift: `RUN_OPTION_KEYS` from
    `omniload.api` (what a run can carry) minus `consumed_run_options()` from
    `FilesystemSource` (what this family declares). The two names in the
    difference are exempted only where they are substrings of a legitimate
    package-local name (`data_item_format` contains no such collision; checked
    directly against word boundaries via a regex, not a bare substring test).
    """
    import re

    package_names = FilesystemSource().consumed_run_options()
    forbidden = sorted(set(RUN_OPTION_KEYS) - package_names)
    assert forbidden, "sanity: the omniload-only set should not be empty"

    package_root = Path(__file__).resolve().parents[2] / "src" / "dlt_filesystem"
    pattern = re.compile(
        r"\b(" + "|".join(re.escape(name) for name in forbidden) + r")\b"
    )

    scanned = sorted(package_root.rglob("*.py"))
    # An empty scan (a moved or renamed package root) would make every assertion
    # below pass vacuously, so the sweep having actually run is part of the gate.
    assert package_root.is_dir(), f"{package_root} is not a directory"
    assert len(scanned) > 30, f"expected dozens of modules, found {len(scanned)}"

    hits: list[str] = []
    for path in scanned:
        text = path.read_text()
        for name in sorted(set(pattern.findall(text))):
            hits.append(f"{path.relative_to(package_root)}: {name}")
    assert hits == [], f"omniload-only run options leaked into the package: {hits}"


#: A fixed modification time, so the second run sees an unchanged file.
STAMP = datetime(2026, 1, 1, tzinfo=timezone.utc)


class MemoryBackedFTP(MemoryFileSystem):
    """An in-memory stand-in for a locator connector's backend.

    Two adjustments make it drivable end to end: paths are stripped for any scheme
    rather than `memory://` alone, and listings carry `mtime`, which is what the
    lister resolves a file's modification date from.
    """

    cachable = False

    @classmethod
    def _strip_protocol(cls, path):
        return "/" + str(path).split("://", 1)[-1].lstrip("/")

    def __init__(self, *args, **kwargs):
        super().__init__()

    def info(self, path, **kwargs):
        return dict(super().info(path, **kwargs), mtime=STAMP)

    def ls(self, path, detail=True, **kwargs):
        listing = super().ls(path, detail=detail, **kwargs)
        if not detail:
            return listing
        return [dict(entry, mtime=STAMP) for entry in listing]


def test_second_run_over_a_locator_connector_loads_nothing_new(tmp_path):
    """The behaviour the dropped flag cost: an unchanged file is read once, not every run.

    Before the boundary existed, `--filesystem-incremental` never reached the
    reference on this path, so every run re-read every file.
    """
    import dlt
    import duckdb

    MemoryBackedFTP().pipe_file("ftp://host:21/bucket/people.csv", b"name\nAlice\n")
    database = tmp_path / "warehouse.duckdb"

    def build_source():
        return FTPSource().dlt_source(
            "ftp://user:pw@host/bucket/people.csv",
            "bucket/people.csv",
            filesystem_incremental=True,
        )

    pipeline = dlt.pipeline(
        pipeline_name="filesystem_option_ownership",
        destination=dlt.destinations.duckdb(str(database)),
        dataset_name="out",
        pipelines_dir=str(tmp_path / "state"),
    )
    with mock.patch("fsspec.implementations.ftp.FTPFileSystem", MemoryBackedFTP):
        pipeline.run(build_source(), table_name="people")
        second = pipeline.run(build_source(), table_name="people")

    assert second.load_packages == []

    connection = duckdb.connect(str(database))
    try:
        rows = connection.sql("select name from out.people").fetchall()
    finally:
        connection.close()
    assert rows == [("Alice",)]


def test_optional_reference_column_types(tmp_path):
    """A run that names no columns leaves the reference field unset."""
    case = Case("ftp", "ftp://user:pw@host/bucket/data.csv")
    reference, _ = _drive(case, tmp_path, column_types=None)

    assert reference.column_types is None
