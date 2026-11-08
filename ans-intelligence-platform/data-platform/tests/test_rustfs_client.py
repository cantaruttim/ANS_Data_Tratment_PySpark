
from unittest.mock import MagicMock, patch

import pytest

from ans_data_platform.config.settings import Settings
from ans_data_platform.storage.rustfs_client import RustFSClient


@pytest.fixture
def settings():
    return Settings(
        s3_endpoint_url="http://localhost:9000",
        s3_bucket_name="ans-data-lake",
        s3_region="us-east-1",
        s3_access_key="test-access-key",
        s3_secret_key="test-secret-key",
    )


@pytest.fixture
def mock_s3_client():
    with patch(
        "ans_data_platform.storage.rustfs_client.boto3.client"
    ) as mock:
        yield mock


def test_list_buckets(settings, mock_s3_client):
    mock_s3_client.return_value.list_buckets.return_value = {
        "Buckets": [
            {"Name": "ans-data-lake"},
            {"Name": "another-bucket"},
        ]
    }

    storage = RustFSClient(settings)

    result = storage.list_buckets()

    assert result == ["ans-data-lake", "another-bucket"]


def test_upload_file(settings, mock_s3_client, tmp_path):
    source_file = tmp_path / "test.csv"
    source_file.write_text("id;value\n1;100")

    storage = RustFSClient(settings)

    storage.upload_file(source_file, "bronze/test.csv")

    storage.client.upload_file.assert_called_once_with(
        str(source_file),
        "ans-data-lake",
        "bronze/test.csv",
    )


def test_upload_file_not_found(settings, mock_s3_client):
    storage = RustFSClient(settings)

    with pytest.raises(FileNotFoundError):
        storage.upload_file(
            "nonexistent.csv",
            "bronze/nonexistent.csv",
        )

    storage.client.upload_file.assert_not_called()


def test_read_object(settings, mock_s3_client):
    mock_body = MagicMock()
    mock_body.read.return_value = b"id;value\n1;100"

    storage = RustFSClient(settings)

    storage.client.get_object.return_value = {
        "Body": mock_body
    }

    result = storage.read_object("bronze/test.csv")

    assert result == b"id;value\n1;100"

    mock_body.close.assert_called_once()

@pytest.fixture
def settings():
    return Settings(
        # RustFS
        s3_endpoint_url="http://localhost:9000",
        s3_bucket_name="ans-data-lake",
        s3_region="us-east-1",
        s3_access_key="test-access-key",
        s3_secret_key="test-secret-key",

        # PostgreSQL
        postgres_host="localhost",
        postgres_port=5434,
        postgres_db="ans_intelligence",
        postgres_user="test-user",
        postgres_password="test-password",
    )