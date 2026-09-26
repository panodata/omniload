# dlt-filesystem

Filesystem and blob storage sources and destinations for
[dlt](https://github.com/dlt-hub/dlt).

`dlt` ships its own filesystem source, and this package is the delta over it:

- **16 readers behind 19 routed format keys**, against dlt's four. CSV (pyarrow,
  DuckDB and a headerless variant), JSON and JSONL, Parquet, ORC, Avro, Feather,
  BSON, spreadsheets (`xlsx` and `ods`), XML, YAML, MessagePack and CBOR.
- **A `filesystem` resource that refuses to load nothing.** A concrete selection
  matching no file raises rather than returning an empty table; globs stay
  empty-safe.
- **A modification-date resolver that covers the schemes dlt's table does not**,
  including `r2`, `oss`, `hdfs`, `smb`, `ftp` and `webdav`, and a pyarrow-backed
  client addressed as `s3://`.
- **An Arrow `readinto` shim**, without which reading a `.gz` fails wherever
  `isal` is importable.

The entry points are a superset of dlt's own, so a pipeline already on
`dlt.sources.filesystem` can move across without changing its call.

## Install

```shell
pip install dlt-filesystem
```

The long-tail formats (XML, YAML, MessagePack, CBOR) carry their decoders in an
extra:

```shell
pip install 'dlt-filesystem[iterable]'
```

Formats whose decoder is a sizeable or single-purpose package carry it in an extra
of their own. Without it, the format still routes and its reader raises an error
naming the extra to install.

| Extra         | Formats                             |
|---------------|-------------------------------------|
| `bson`        | BSON                                |
| `duckdb`      | CSV read with DuckDB (`csv_duckdb`) |
| `spreadsheet` | XLSX (read and write), ODS          |
| `vortex`      | Vortex (Python 3.11+)               |

```shell
pip install 'dlt-filesystem[spreadsheet]'
```

## Usage

```python
import dlt
from dlt_filesystem.source.adapter import readers

pipeline = dlt.pipeline(destination="duckdb", dataset_name="inbox")
pipeline.run(
    readers(bucket_url="s3://bucket/prefix", file_glob="*.parquet").read_parquet()
)
```

## Documentation

<https://omniload.readthedocs.io/supported-sources/filesystem.html>

## License

MIT. See [LICENSE](https://github.com/panodata/omniload/blob/main/LICENSE).
