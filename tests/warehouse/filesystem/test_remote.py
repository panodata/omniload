import csv
import gzip
import io
import json
from typing import Callable, Iterable
from unittest.mock import patch

import pyarrow.csv
import pytest
import sqlalchemy
from dlt.common.storages.fsspec_filesystem import glob_files
from fsspec.implementations.memory import MemoryFileSystem
from pyarrow import parquet as pya_parquet

from omniload.error import InvalidBlobTableError
from omniload.target.filesystem import S3Destination
from tests.util import invoke_ingest_command
from tests.util.common import get_random_string, has_exception
from tests.warehouse.settings import DESTINATIONS


def fs_test_cases(
    protocol: str,
    target_fs: str,
    auth: str,
) -> Iterable[Callable]:
    """
    Tests for filesystem based sources
    """
    testdata = (
        "name,phone,email,country\n"
        "Rajah Roach,1-459-646-7421,adipiscing.ligula@outlook.net,Austria\n"
        "Kiayada Jackson,(341) 484-6523,velit.egestas.lacinia@hotmail.couk,Norway\n"
        "Bradley Grant,1-329-268-4178,leo.cras@hotmail.org,Chile\n"
        "Damian Velasquez,(462) 744-9637,phasellus.fermentum@outlook.ca,South Africa\n"
        "Rina Nicholson,(201) 971-6463,neque.nullam.ut@yahoo.net,Brazil\n"
    )
    testdata_extended = (
        "name,phone,email,country\n"
        "Irene Douglas,(223) 971-6463,flying.fish.kick@gmail.com,UK\n"
    )
    test_fs = MemoryFileSystem()

    # for CSV tests
    with test_fs.open("/data.csv", "w") as f:
        f.write(testdata)
    with test_fs.open("/data.csv.gz", "wb") as f:
        with gzip.GzipFile(fileobj=f, mode="wb") as gz:
            gz.write(testdata.encode())

    # for Glob tests
    with test_fs.open("/data2.csv", "w") as f:
        f.write(testdata_extended)

    # For Parquet tests
    with test_fs.open("/data.parquet", "wb") as f:
        table = pyarrow.csv.read_csv(io.BytesIO(testdata.encode()))
        pya_parquet.write_table(table, f)
    with io.BytesIO() as buf:
        pya_parquet.write_table(table, buf)
        buf.seek(0)
        with test_fs.open("/data.parquet.gz", "wb") as f:
            with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                gz.write(buf.getvalue())

    # For JSONL tests
    with test_fs.open("/data.jsonl", "w") as f:
        reader = csv.DictReader(io.StringIO(testdata))
        for row in reader:
            json.dump(row, f)
            f.write("\n")
    with test_fs.open("/data.jsonl.gz", "wb") as f:
        with gzip.GzipFile(fileobj=f, mode="wb") as gz:
            reader = csv.DictReader(io.StringIO(testdata))
            for row in reader:
                gz.write(json.dumps(row).encode())
                gz.write(b"\n")

    # for testing unsupported files
    with test_fs.open("/bin/data.bin", "w") as f:
        f.write("BINARY")

    def glob_files_override(fs_client, _, file_glob):
        return glob_files(fs_client, "memory://", file_glob)

    def assert_rows(dest_uri, dest_table, n):
        engine = sqlalchemy.create_engine(dest_uri)
        with engine.connect() as conn:
            rows = conn.exec_driver_sql(f"select count(*) from {dest_table}").fetchall()
        engine.dispose()
        assert len(rows) == 1
        assert rows[0] == (n,)

    def test_empty_source_uri(dest_uri):
        """
        When the source URI is empty, an error should be raised.
        """
        schema = f"testschema_fs_{get_random_string(5)}"
        result = invoke_ingest_command(
            f"{protocol}://bucket?{auth}",
            "",
            dest_uri,
            f"{schema}.test",
            print_output=False,
        )
        assert has_exception(result.exception, InvalidBlobTableError)

    def test_unsupported_file_format(dest_uri):
        """
        When the source file is not one of [csv, parquet, jsonl] it should
        raise an exception
        """
        with (
            patch(target_fs),
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/bin/data.bin",
                dest_uri,
                dest_table,
                print_output=False,
            )
            assert result.exit_code != 0
            assert has_exception(result.exception, ValueError)

    def test_missing_credentials(dest_uri):
        """
        When the credentials are missing, an error should be raised.
        """
        schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
        dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
        result = invoke_ingest_command(
            f"{protocol}://bucket",
            "/data.csv",
            dest_uri,
            dest_table,
            print_output=False,
        )
        assert result.exit_code != 0

    def test_csv_load(dest_uri):
        """
        When the source URI is a CSV file, the data should be ingested.
        """
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.csv",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_csv_gz_load(dest_uri):
        """When the source URI is a gzipped CSV file, the data should be ingested."""
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.csv.gz",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_parquet_load(dest_uri):
        """
        When the source URI is a Parquet file, the data should be ingested.
        """
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.parquet",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_parquet_gz_load(dest_uri):
        """When the source URI is a gzipped Parquet file, the data should be ingested."""
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.parquet.gz",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_jsonl_load(dest_uri):
        """
        When the source URI is a JSONL file, the data should be ingested.
        """
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.jsonl",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_jsonl_gz_load(dest_uri):
        """When the source URI is a gzipped JSONL file, the data should be ingested."""
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/data.jsonl.gz",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 5)

    def test_glob_load(dest_uri):
        """
        When the source URI is a glob pattern, all files matching the pattern should be ingested
        """
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket?{auth}",
                "/*.csv",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 6)

    def test_compound_table_name(dest_uri):
        """
        When table contains both the bucket name and the file glob,
        loads should be successful.
        """
        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://?{auth}",
                "bucket/*.csv",
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 6)

    def test_uri_precedence(dest_uri):
        """
        When file glob is present in both URI and Source Table,
        the URI glob should be used
        """

        with (
            patch(target_fs) as target_fs_mock,
            patch(
                "omniload.source.filesystem.adapter.glob_files",
                wraps=glob_files_override,
            ),
        ):
            target_fs_mock.return_value = test_fs
            schema_rand_prefix = f"testschema_fs_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.fs_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"{protocol}://bucket/*.csv?{auth}",
                "/path/to/file",  # if this is used, it should result in an error
                dest_uri,
                dest_table,
            )
            assert result.exit_code == 0
            assert_rows(dest_uri, dest_table, 6)

    return [
        test_empty_source_uri,
        test_missing_credentials,
        test_unsupported_file_format,
        test_csv_load,
        test_csv_gz_load,
        test_parquet_load,
        test_parquet_gz_load,
        test_jsonl_load,
        test_jsonl_gz_load,
        test_glob_load,
        test_compound_table_name,
        test_uri_precedence,
    ]


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize(
    "test_case",
    fs_test_cases(
        "gs",
        "gcsfs.GCSFileSystem",
        "credentials_base64=e30K",  # base 64 for "{}"
    ),
)
def test_gcs(dest, test_case):
    test_case(dest.start())
    dest.stop()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize(
    "test_case",
    fs_test_cases(
        "s3",
        "s3fs.S3FileSystem",
        "access_key_id=KEY&secret_access_key=SECRET",
    ),
)
def test_s3(dest, test_case):
    test_case(dest.start())
    dest.stop()


def test_s3_destination():
    # should raise an error if endpoint_url doesn't have a scheme or a host
    with pytest.raises(ValueError, match="Invalid endpoint_url"):
        S3Destination().dlt_dest(
            uri="s3://?access_key_id=KEY&secret_access_key=SECRET&endpoint_url=localhost:9000",
            dest_table="bucket/test_table",
        )
