import pytest

from omniload.target.blobstorage import S3Destination


def test_s3_destination():
    # should raise an error if endpoint_url doesn't have a scheme or a host
    with pytest.raises(ValueError, match="Invalid endpoint_url"):
        S3Destination().dlt_dest(
            uri="s3://?access_key_id=KEY&secret_access_key=SECRET&endpoint_url=localhost:9000",
            dest_table="bucket/test_table",
        )
