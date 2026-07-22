(dbfs)=
(databricks-files)=

# Databricks files

[Databricks] is a platform for big data analytics and artificial intelligence.
`omniload` supports access to files on Databricks volumes and workspaces as a
source.

- Unity Catalog Volumes
- Workspace files
- Legacy DBFS (Databricks File System)

## URI format

The URI for connecting to Databricks files is structured as follows.

```text
dbfs:/Volumes/catalog/schema/volume/path/to/data.parquet
```
```text
dbfs:/Workspace/path/to/data.parquet
```

## URI parameters

:host:
  See `Databricks native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#databricks-native-authentication>`_.

:account_id:
  See `Databricks native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#databricks-native-authentication>`_.

:token:
  See `Databricks native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#databricks-native-authentication>`_.

:username:
  See `Databricks native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#databricks-native-authentication>`_.

:password:
  See `Databricks native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#databricks-native-authentication>`_.

:client_id:
  (Service principal OAuth only) The client ID you were assigned when creating your service principal.

:client_secret:
  (Service principal OAuth only) The client secret you generated when creating your service principal.

:azure_workspace_resource_id:
  See `Azure native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#azure-native-authentication>`_.

:azure_client_secret:
  See `Azure native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#azure-native-authentication>`_.

:azure_client_id:
  See `Azure native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#azure-native-authentication>`_.

:azure_tenant_id:
  See `Azure native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#azure-native-authentication>`_.

:azure_environment:
  See `Azure native authentication <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#azure-native-authentication>`_.

:auth_type:
  See `Additional configuration options <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#additional-configuration-options>`_.

:cluster_id:
  The ID of the cluster to connect. See `Compute configuration for Databricks Connect <https://docs.databricks.com/aws/en/dev-tools/databricks-connect/cluster-config>`_.

:profile:
  See `Overriding .databrickscfg <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#overriding-databrickscfg>`_.

:config_file:
  See `Overriding .databrickscfg <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#overriding-databrickscfg>`_.

:debug_headers:
  See `Additional configuration options <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#additional-configuration-options>`_.
  Type: `bool`. Default: `false`.

:debug_truncate_bytes:
  See `Additional configuration options <https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html#additional-configuration-options>`_.
  Type: `int`.

:volume_fs_max_read_concurrency:
  The maximum number of concurrent file read operations on a Unity Catalog Volume file.
  Type: `int`. Default: `24`.

:volume_fs_min_read_block_size:
  The minimum data size to read for each read operation on a Unity Catalog Volume file.
  Type: `int`. Default: `1048576` (1 MB).

:volume_fs_max_read_block_size:
  The maximum data size to read for each read operation on a Unity Catalog Volume file.
  Type: `int`. Default: `4194304` (4 MB).

:volume_fs_max_write_concurrency:
  The maximum number of concurrent file write operations on a Unity Catalog Volume file.
  Type: `int`. Default: `24`.

:volume_fs_min_write_block_size:
  The minimum data size to write for each write operation on a Unity Catalog Volume file.
  Type: `int`. Default: `5242880` (5 MB).

:volume_fs_max_write_block_size:
  The maximum data size to write for each write operation on a Unity Catalog Volume file.
  Type: `int`. Default: `16777216` (16 MB).

:volume_min_multipart_upload_size:
  The minimum file size to use multipart upload for uploading files to Unity Catalog Volume.
  Type: `int`. Default: `5242880` (5 MB).

:volume_fs_connection_pool_size:
  The maximum number of connections in the aiohttp connection pool for the Unity Catalog Volume file system.
  Type: `int`. Default: `100`.

:use_local_fs_in_workspace:
  Access files from the local file system rather than the remote Databricks API when running within a Databricks workspace.
  Type: `bool`. Default: `true`.

:verbose_debug_log:
  Whether to enable verbose debug logging for file system operations.
  Type: `bool`. Default: `false`.

## Authentication

The Databricks connector `fsspec-databricks` uses Databricks Unified
Authentication provided by the Databricks Python SDK. You can find information
about supported authentication parameters and environment variables in the
[Databricks Python SDK documentation].

## Example: Load Parquet file from Databricks into DuckDB

```sh
omniload ingest \
    --source-uri   'dbfs:/Volumes/catalog/schema/volume/path/to/data.parquet' \
    --dest-uri     'duckdb:///demo.duckdb' \
    --dest-table   'testdrive.data'
```

Running the command creates a table named `data` within the `testdrive`
schema in the DuckDB database file located at `demo.duckdb`.


[Databricks]: https://www.databricks.com/
[Databricks Python SDK documentation]: https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html
