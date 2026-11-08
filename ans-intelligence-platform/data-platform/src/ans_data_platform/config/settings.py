
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[4]

load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    # Object Storage - RustFS
    s3_endpoint_url: str
    s3_bucket_name: str
    s3_region: str
    s3_access_key: str
    s3_secret_key: str

    # Relational Database - PostgreSQL
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @classmethod
    def from_env(cls):
        required_variables = {
            "S3_ENDPOINT_URL": os.getenv("S3_ENDPOINT_URL"),
            "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME"),
            "S3_REGION": os.getenv("S3_REGION"),
            "RUSTFS_ACCESS_KEY": os.getenv("RUSTFS_ACCESS_KEY"),
            "RUSTFS_SECRET_KEY": os.getenv("RUSTFS_SECRET_KEY"),
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
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

        try:
            postgres_port = int(required_variables["POSTGRES_PORT"])
        except ValueError as exc:
            raise ValueError(
                "POSTGRES_PORT must be a valid integer."
            ) from exc

        if not 1 <= postgres_port <= 65535:
            raise ValueError(
                "POSTGRES_PORT must be between 1 and 65535."
            )

        return cls(
            # RustFS
            s3_endpoint_url=required_variables["S3_ENDPOINT_URL"],
            s3_bucket_name=required_variables["S3_BUCKET_NAME"],
            s3_region=required_variables["S3_REGION"],
            s3_access_key=required_variables["RUSTFS_ACCESS_KEY"],
            s3_secret_key=required_variables["RUSTFS_SECRET_KEY"],

            # PostgreSQL
            postgres_host=required_variables["POSTGRES_HOST"],
            postgres_port=postgres_port,
            postgres_db=required_variables["POSTGRES_DB"],
            postgres_user=required_variables["POSTGRES_USER"],
            postgres_password=required_variables["POSTGRES_PASSWORD"],
        )
