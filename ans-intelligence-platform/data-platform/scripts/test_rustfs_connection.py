
import logging
from pathlib import Path

from ans_data_platform.config.logging_config import configure_logging
from ans_data_platform.config.settings import Settings
from ans_data_platform.storage.rustfs_client import RustFSClient


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    # 1. Configure application logging
    configure_logging()

    logger.info("Starting RustFS connection validation")

    # 2. Load application settings
    settings = Settings.from_env()

    # 3. Initialize RustFS client
    storage = RustFSClient(settings)

    # 4. Validate bucket access
    buckets = storage.list_buckets()

    logger.info("Available buckets: %s", buckets)

    if settings.s3_bucket_name not in buckets:
        raise RuntimeError(
            f"Bucket '{settings.s3_bucket_name}' was not found."
        )

    logger.info(
        "Target bucket '%s' is accessible.",
        settings.s3_bucket_name,
    )

    # 5. Upload test file
    source_file = (
        PROJECT_ROOT
        / "tests"
        / "fixtures"
        / "ans-rustfs-validation.txt"
    )

    if not source_file.is_file():
        raise FileNotFoundError(
            f"Test file not found: {source_file}"
        )

    object_key = "validation/python/ans-rustfs-validation.txt"

    storage.upload_file(source_file, object_key)

    # 6. Read stored object
    stored_content = storage.read_object(object_key)
    original_content = source_file.read_bytes()

    # 7. Validate content
    if stored_content != original_content:
        raise RuntimeError("Content validation failed!")

    logger.info("Content validation passed!")
    logger.info("RustFS connection validation completed successfully.")


if __name__ == "__main__":
    main()
