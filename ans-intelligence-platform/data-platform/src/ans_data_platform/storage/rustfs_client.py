
import logging
from pathlib import Path

import boto3
from botocore.config import Config

from ans_data_platform.config.settings import Settings

logger = logging.getLogger(__name__)

class RustFSClient:
    def __init__(self, settings: Settings):
        self.bucket_name = settings.s3_bucket_name

        self.client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        )

    def list_buckets(self) -> list[str]:
        response = self.client.list_buckets()

        return [
            bucket["Name"]
            for bucket in response.get("Buckets", [])
        ]

    def upload_file(
        self,
        file_path: str | Path,
        object_key: str,
    ) -> None:
        source = Path(file_path)

        if not source.is_file():
            raise FileNotFoundError(
                f"File not found: {source}"
            )

        self.client.upload_file(
            str(source),
            self.bucket_name,
            object_key,
        )

    def read_object(self, object_key: str) -> bytes:
        response = self.client.get_object(
            Bucket=self.bucket_name,
            Key=object_key,
        )

        body = response["Body"]

        try:
            return body.read()
        finally:
            body.close()


def list_buckets(self) -> list[str]:
    logger.debug("Listing available buckets")

    response = self.client.list_buckets()

    buckets = [
        bucket["Name"]
        for bucket in response.get("Buckets", [])
    ]

    logger.info("Retrieved %d buckets", len(buckets))

    return buckets


def upload_file(
    self,
    file_path: str | Path,
    object_key: str,
) -> None:
    source = Path(file_path)

    if not source.is_file():
        logger.warning("File not found: %s", source)
        raise FileNotFoundError(
            f"File not found: {source}"
        )

    logger.info(
        "Starting upload: %s",
        object_key,
    )

    self.client.upload_file(
        str(source),
        self.bucket_name,
        object_key,
    )

    logger.info(
        "Upload completed: %s",
        object_key,
    )


def read_object(self, object_key: str) -> bytes:
    logger.info(
        "Reading object: %s",
        object_key,
    )

    response = self.client.get_object(
        Bucket=self.bucket_name,
        Key=object_key,
    )

    body = response["Body"]

    try:
        content = body.read()

        logger.info(
            "Object read successfully: %s (%d bytes)",
            object_key,
            len(content),
        )

        return content

    finally:
        body.close()
