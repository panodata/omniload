(hdfs)=

# HDFS

[Hadoop distributed file system (HDFS)] is a distributed, scalable, and
portable file system written in Java for the Hadoop framework.

`omniload` supports HDFS as a data source.

## URI Format

The URI for connecting to HDFS is structured as follows.

```text
hdfs://example.com:8020/path/to/data.parquet?user=test
```

## URI components

:host:
  HDFS host to connect to. Set to "default" for `fs.defaultFS` from
  `core-site.xml`.

:port:
  HDFS port to connect to.
  Type: `int`. Default: `8020`.

:user:
  Username when connecting to HDFS; None implies login user.

:replication:
  Number of copies each block will have.
  Type: `int`. Default: `3`.

:buffer_size:
  The size of the temporary read and write buffer in bytes.
  `0` means no buffering will happen.
  Type: `int`. Default: `0`.

:block_size:
  The block size in bytes. `None` means the default configuration
  for HDFS, a typical block size is 128 MB.
  Type: `int`. Default: `None`.

:kerb_ticket:
  The path to the Kerberos ticket cache.

:extra_conf:
  Optional extra key/value pairs for configuration; will override any
  `hdfs-site.xml` properties.
  Type: `dict`. Use JSON to encode the dictionary.

## Examples

To integrate `omniload` with HDFS, you need the server's hostname (endpoint)
and valid credentials.

### Load Parquet data from HDFS into DuckDB

The following command demonstrates how to copy data from a specified HDFS
location into a DuckDB database.

```sh
omniload ingest \
    --source-uri   'hdfs://example.com:8020/?user=test' \
    --source-table 'path/to/data.parquet' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'public.example'
```

Running the command creates a table named `example` within the `public` schema
(or equivalent grouping) in the DuckDB database file located at `demo.duckdb`.

:::{tip}
Here, instead of defining the remote resource exclusively per source URI
using its `<path>` component, the resource location is specified using the
separate `--source-table` option. Both addressing variants are supported equally.
:::

## Backlog

:::{todo}
PyArrow comes with bindings to the Hadoop File System, however you must
still [configure it properly]. In this spirit, because no packaging
efforts were poured into this, the HDFS connector can not be expected
to work out of the box, for example when using the omniload OCI image.
Please [create an issue] to ping us about any improvement needs.
:::


[configure it properly]: https://arrow.apache.org/docs/python/filesystems.html#hadoop-distributed-file-system-hdfs
[create an issue]: https://github.com/panodata/omniload/issues
[Hadoop distributed file system (HDFS)]: https://en.wikipedia.org/wiki/Apache_Hadoop#HDFS
