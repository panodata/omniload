(http)=

# HTTP

`omniload` reads a file addressed by an [HTTP] or HTTPS URL, using the same reader
stack as every other {ref}`filesystem <filesystem>` source. See that page for the
supported formats, format and reader hints, and type normalization.

## URI format

The URI is the URL of the file, exactly as you would open it.

```text
https://example.org/path/to/data.csv
http://example.org/path/to/data.parquet
```

## The query string is part of the address

Unlike the other schemes in this family, an HTTP URL's query string is **not**
connection configuration: it is sent to the server. That is what makes a presigned
URL work, since the signature lives in the query and is computed over its encoded
form, and an escape that carries meaning (`%2F`) is preserved rather than decoded.
An escape of an unreserved character is normalized (`%7E` becomes `~`), which every
canonical signing scheme treats as the same value:

```sh
omniload ingest \
    --source-uri   'https://bucket.s3.amazonaws.com/exports/orders.csv?X-Amz-Signature=...&X-Amz-Expires=900' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.orders'
```

Quote the URI in your shell: `&` would otherwise background the command.

:::{note}
Because the query is addressing information, connection options cannot ride in it,
the way they do for `s3://` and the other schemes. They are available when you
build the source yourself, where a keyword argument reaches the underlying [fsspec
HTTP filesystem]:

```python
from dlt_filesystem.source.fsspec.http import HttpFilesystemSource

source = HttpFilesystemSource().dlt_source(
    "https://example.org/data.jsonl", "", block_size=65536
)
```

`omniload ingest` and `run_ingest` take no option of their own for these, but
fsspec's own environment configuration reaches the same constructor from either:
`FSSPEC_HTTP_BLOCK_SIZE=65536` for a scalar, or `FSSPEC_HTTP` holding a JSON
object for anything structured. `client_kwargs` is the one exception, because
this source always passes its own.

A block size of `0` is answered with the whole body in one request rather than
with fsspec's streaming file, whichever route sets it. That file cannot seek and
reports `seekable()` as `True` anyway, so the readers that seek (Parquet, ORC,
Feather, JSONL and a headerless CSV) failed on it rather than on the open. The
cost is memory, and only for BSON and MessagePack: they were the two readers
that worked on the streaming handle by reading it forward in pieces, and they
now hold the whole body instead. Every other reader either seeks or reads the
body in one call already.
:::

## Authentication

Credentials in the URL's userinfo become an HTTP basic-authentication header.
Percent-encode any character that a URI cannot carry literally, `@` and `/`
included:

```text
https://<USERNAME>:<PASSWORD>@example.org/private/data.csv
```

Environment proxy settings and `.netrc` are honoured.

## Extended type support

| Type          | Support | Remarks                                            |
|:--------------|:--------|:---------------------------------------------------|
| Formats       | ✅      | See {ref}`filesystem <filesystem>`.                |
| Ranges        | ✅      | Used when the server supports them.                |
| Globs         | ✅      | Requires an HTML directory index.                  |
| Last modified | ✅      | Requires valid `Last-Modified` on each file.       |

## Examples

### Load a public CSV file into DuckDB

```sh
omniload ingest \
    --source-uri   'https://example.org/path/to/data.csv' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

### Name the format for a URL that has no useful extension

An API endpoint rarely ends in `.csv`. Append a `#format` fragment:

```sh
omniload ingest \
    --source-uri   'https://api.example.org/exports/latest#csv' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

## Directory indexes and wildcards

A server that exposes an HTML directory index can be read with a glob.
`*` selects links in one directory, while `**` follows linked subdirectories
recursively:

```sh
omniload ingest \
    --source-uri   'https://example.org/exports/**/*.csv' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.exports'
```

The server's index defines what can be discovered. A wildcard cannot find files
that the index does not link.

## Incremental file selection

Add `--filesystem-incremental` to append rows only from new or modified files, as
described on the {ref}`filesystem <filesystem>` page. Each selected file must
return a valid `Last-Modified` header. A directory index does not carry that
metadata, so an incremental glob makes one metadata request per matched file. A
missing or malformed header stops extraction and names the query-free file URL.
A plain, non-incremental load still works when the header is absent.

## How much is transferred

Where the server honours byte ranges, the file is read in ranges rather than
downloaded whole, so a line-delimited document starts producing rows before all of
it has arrived. Two cases are read whole instead, because they cannot be read any
other way:

- a server that ignores `Range` and answers with the entire body;
- a response with no `Content-Length` (ordinary `Transfer-Encoding: chunked`),
  which leaves the file with no known size and therefore no seekable form.

Parquet is read whole in every case: pyarrow asks for a file's entire data section
in one read, independently of the transport.

## Limitations

- **Read only.** There is no portable way to write a file over plain HTTP.

See the {ref}`WebDAV <webdav>` source for an HTTP-based transport with a defined
listing protocol.

[fsspec HTTP filesystem]: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.http.HTTPFileSystem
[HTTP]: https://developer.mozilla.org/en-US/docs/Web/HTTP
