
import os
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv


# 1. Load project environment variables
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


# 2. Read configuration
endpoint_url = os.getenv("S3_ENDPOINT_URL")
bucket_name = os.getenv("S3_BUCKET_NAME")
region = os.getenv("S3_REGION")

access_key = os.getenv("RUSTFS_ACCESS_KEY")
secret_key = os.getenv("RUSTFS_SECRET_KEY")


# 3. Validate required configuration
required = {
    "S3_ENDPOINT_URL": endpoint_url,
    "S3_BUCKET_NAME": bucket_name,
    "S3_REGION": region,
    "RUSTFS_ACCESS_KEY": access_key,
    "RUSTFS_SECRET_KEY": secret_key,
}

missing = [name for name, value in required.items() if not value]

if missing:
    raise RuntimeError(
        f"Missing environment variables: {', '.join(missing)}"
    )


# 4. Create S3 client
s3_client = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region,
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)


# 5. List available buckets
response = s3_client.list_buckets()

buckets = [bucket["Name"] for bucket in response["Buckets"]]

print("Connection established successfully!")
print("Available buckets:", buckets)


# 6. Validate target bucket
if bucket_name not in buckets:
    raise RuntimeError(
        f"Bucket '{bucket_name}' was not found."
    )

print(f"Target bucket '{bucket_name}' is accessible.")


# 7. Upload a test file
source_file = (
    PROJECT_ROOT
    / "tests"
    / "fixtures"
    / "ans-rustfs-validation.txt"
)

if not source_file.is_file():
    raise FileNotFoundError(f"Test file not found: {source_file}")

object_key = "validation/python/ans-rustfs-validation.txt"

s3_client.upload_file(
    str(source_file),
    bucket_name,
    object_key,
)

print(f"Upload successful: {object_key}")


# 8. Read the object directly from RustFS
response = s3_client.get_object(
    Bucket=bucket_name,
    Key=object_key,
)

stored_content = response["Body"].read()
original_content = source_file.read_bytes()

# 9. Compare original and stored content
if stored_content != original_content:
    raise RuntimeError("Content validation failed!")

print("Read successful!")
print("Content validation passed!")
