
import pytest

from ans_data_platform.config.settings import Settings
from ans_data_platform.storage.rustfs_client import RustFSClient


@pytest.fixture
def rustfs_context():
    settings = Settings.from_env()
    storage = RustFSClient(settings)

    return settings, storage


@pytest.mark.integration
def test_rustfs_upload_and_read_round_trip(
    rustfs_context,
    tmp_path,
):
    # 1. Get storage configuration and client
    settings, storage = rustfs_context

    # 2. Verify bucket availability
    buckets = storage.list_buckets()

    assert settings.s3_bucket_name in buckets

    # 3. Create a temporary file
    original_content = (
        b"ANS Intelligence Platform - Integration Test"
    )

    source_file = tmp_path / "integration-test.txt"
    source_file.write_bytes(original_content)

    # 4. Upload file to RustFS
    object_key = "validation/integration/pytest-roundtrip.txt"

    storage.upload_file(source_file, object_key)

    # 5. Read object from RustFS
    stored_content = storage.read_object(object_key)

    # 6. Validate content integrity
    assert stored_content == original_content
