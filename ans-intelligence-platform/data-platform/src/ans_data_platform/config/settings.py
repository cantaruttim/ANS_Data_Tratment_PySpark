
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[4]

load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    s3_endpoint_url: str
    s3_bucket_name: str
    s3_region: str
    s3_access_key: str
    s3_secret_key: str

    @classmethod
    def from_env(cls):
        required_variables = {
            "S3_ENDPOINT_URL": os.getenv("S3_ENDPOINT_URL"),
            "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME"),
            "S3_REGION": os.getenv("S3_REGION"),
            "RUSTFS_ACCESS_KEY": os.getenv("RUSTFS_ACCESS_KEY"),
            "RUSTFS_SECRET_KEY": os.getenv("RUSTFS_SECRET_KEY"),
        }

        missing = [
            name
            for name, value in required_variables.items()
            if not value
        ]

        if missing:
            raise RuntimeError(
                f"Missing environment variables: {', '.join(missing)}"
            )

        return cls(
            s3_endpoint_url=required_variables["S3_ENDPOINT_URL"],
            s3_bucket_name=required_variables["S3_BUCKET_NAME"],
            s3_region=required_variables["S3_REGION"],
            s3_access_key=required_variables["RUSTFS_ACCESS_KEY"],
            s3_secret_key=required_variables["RUSTFS_SECRET_KEY"],
        )
