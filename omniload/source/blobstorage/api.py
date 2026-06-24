import base64
import json
from urllib.parse import parse_qs, urlparse

from omniload.error import InvalidBlobTableError, MissingValueError
from omniload.source.endpoint import (
    UnsupportedEndpointError,
    determine_endpoint,
    parse_uri,
)


class GCSSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "GCS takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("GCS")

        bucket_url = f"gs://{bucket_name}"

        credentials_path = params.get("credentials_path")
        credentials_base64 = params.get("credentials_base64")
        credentials_available = any(
            map(
                lambda x: x is not None,
                [credentials_path, credentials_base64],
            )
        )
        if credentials_available is False:
            raise MissingValueError("credentials_path or credentials_base64", "GCS")

        credentials = None
        if credentials_path:
            credentials = credentials_path[0]
        else:
            credentials = json.loads(base64.b64decode(credentials_base64[0]).decode())  # type: ignore

        # There's a compatiblity issue between google-auth, dlt and gcsfs
        # that makes it difficult to use google.oauth2.service_account.Credentials
        # (The RECOMMENDED way of passing service account credentials)
        # directly with gcsfs. As a workaround, we construct the GCSFileSystem
        # and pass it directly to filesystem.readers.
        import gcsfs

        fs = gcsfs.GCSFileSystem(
            token=credentials,
        )

        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(
                "GCS Source only supports specific formats files: csv, csv_headless, jsonl, parquet"
            )
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        # Handle csv_headless with column_names
        if endpoint == "read_csv_headless":
            from typing import Any, Iterator

            import dlt
            from dlt.sources import TDataItems
            from dlt.sources.filesystem import FileItemDict

            from omniload.source.filesystem.adapter import filesystem
            from omniload.source.filesystem.readers import _read_csv_headless

            column_types = kwargs.get("column_types")
            column_names = list(column_types.keys()) if column_types else None

            def read_csv_headless_with_cols(
                items: Iterator[FileItemDict],
                chunksize: int = 10000,
                **pandas_kwargs: Any,
            ) -> Iterator[TDataItems]:
                yield from _read_csv_headless(
                    items,
                    chunksize=chunksize,
                    column_names=column_names,
                    **pandas_kwargs,
                )

            filesystem_resource = filesystem(bucket_url, fs, file_glob=path_to_file)
            return filesystem_resource | dlt.transformer(
                name="read_csv_headless", max_table_nesting=0
            )(read_csv_headless_with_cols)

        from omniload.source.filesystem.adapter import readers

        return readers(bucket_url, fs, path_to_file).with_resources(endpoint)


class S3Source:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "S3 takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        source_fields = parse_qs(parsed_uri.query)
        access_key_id = source_fields.get("access_key_id")
        if not access_key_id:
            raise ValueError("access_key_id is required to connect to S3")

        secret_access_key = source_fields.get("secret_access_key")
        if not secret_access_key:
            raise ValueError("secret_access_key is required to connect to S3")

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("S3")

        bucket_url = f"s3://{bucket_name}/"

        import s3fs

        endpoint_url = source_fields.get("endpoint_url")
        fs_kwargs: dict = {
            "key": access_key_id[0],
            "secret": secret_access_key[0],
        }
        if endpoint_url:
            fs_kwargs["endpoint_url"] = endpoint_url[0]

        fs = s3fs.S3FileSystem(**fs_kwargs)

        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(
                "S3 Source only supports specific formats files: csv, csv_headless, jsonl, parquet"
            )
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        # Handle csv_headless with column_names
        if endpoint == "read_csv_headless":
            from typing import Any, Iterator

            import dlt
            from dlt.sources import TDataItems
            from dlt.sources.filesystem import FileItemDict

            from omniload.source.filesystem.adapter import filesystem
            from omniload.source.filesystem.readers import _read_csv_headless

            column_types = kwargs.get("column_types")
            column_names = list(column_types.keys()) if column_types else None

            def read_csv_headless_with_cols(
                items: Iterator[FileItemDict],
                chunksize: int = 10000,
                **pandas_kwargs: Any,
            ) -> Iterator[TDataItems]:
                yield from _read_csv_headless(
                    items,
                    chunksize=chunksize,
                    column_names=column_names,
                    **pandas_kwargs,
                )

            filesystem_resource = filesystem(bucket_url, fs, file_glob=path_to_file)
            return filesystem_resource | dlt.transformer(
                name="read_csv_headless", max_table_nesting=0
            )(read_csv_headless_with_cols)

        from omniload.source.filesystem.adapter import readers

        return readers(bucket_url, fs, path_to_file).with_resources(endpoint)
