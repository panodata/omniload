(oss)=

# OSS

[Object Storage Service (OSS)] is a scalable cloud storage service offered by
Alibaba Cloud. It is a fully managed object storage service to store and
access any amount of data from anywhere. For more information, see the
[OSS getting started FAQ].

`omniload` supports OSS as a data source.

## URI Format

The URI for connecting to OSS is structured as follows. Either access the resource
anonymously, use key/secret credentials, or a security token for authentication.
In this form, the remote resource is addressed exclusively using a single
parameter, an URI, encoding all required options.

:Anonymous access:
  ```text
  oss://bucket/path/to/data.parquet?endpoint=http://oss-cn-hangzhou.aliyuncs.com/
  ```
:Authenticate with key and secret:
  ```text
  oss://bucket/path/to/data.parquet?endpoint=https://oss-me-east-1.aliyuncs.com/&key=foo&secret=bar
  ```
:Authenticate with security token:
  ```text
  oss://bucket/path/to/data.parquet?endpoint=https://oss-me-east-1.aliyuncs.com/&token=foobar
  ```

## URI components

:\<path\>:
  The path to the resource can reference one or multiple files using
  file globbing.

:endpoint:
  OSS endpoint location that can be changed after the initialization.
  Can alternatively be defined using the `OSS_ENDPOINT` environment variable.
  Examples: `http://oss-cn-hangzhou.aliyuncs.com` or `https://oss-me-east-1.aliyuncs.com`.

:key:
  If not anonymous, use this access key ID, when specified.

:secret:
  If not anonymous, use this secret access key, when specified.

:token:
  If not anonymous, use this security token, when specified.

:block_size:
  The block size in bytes. Default: `5242880` (`5MB`).

:cache_type:
  The cache type value used for `open()`.
  Set to `none` to disable caching.
  The default value is `readahead`.
  For other cache types, please refer to the `fsspec` package.

## Set up an OSS integration

Before you use Alibaba Cloud OSS, make sure you have registered an Alibaba
Cloud account. For instructions, see [create an Alibaba Cloud account].
After you create an Alibaba Cloud account, [activate OSS].

## Examples

To integrate `omniload` with OSS, you need the server's hostname (endpoint)
and valid credentials.

### Load CSV data from OSS into DuckDB

The following command demonstrates how to copy data from a specified OSS
location into a DuckDB database.

```sh
omniload ingest \
    --source-uri   'oss://?endpoint=http://oss-cn-hangzhou.aliyuncs.com/' \
    --source-table 'path/to/data.parquet' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'public.example'
```

Running the command creates a table named `example` within the `public` schema
(or equivalent grouping) in the DuckDB database file located at `demo.duckdb`.

:::{tip}
Here, instead of defining the remote resource exclusively per source URI
using its `<path>` component, the bucket name and the file glob pattern
are specified using the separate `--source-table` option. Both addressing
variants are supported equally.
:::


[activate OSS]: https://oss.console.alibabacloud.com/overview
[create an Alibaba Cloud account]: https://account.alibabacloud.com/register/intl_register.htm
[Object Storage Service (OSS)]: https://www.alibabacloud.com/en/product/object-storage-service
[OSS getting started FAQ]: https://www.alibabacloud.com/help/en/oss/faq-15
