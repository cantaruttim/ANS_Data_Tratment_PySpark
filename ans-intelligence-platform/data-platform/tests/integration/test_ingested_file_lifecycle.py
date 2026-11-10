
import hashlib
from uuid import uuid4

import pytest
from psycopg.errors import CheckViolation

from ans_data_platform.config.settings import Settings
from ans_data_platform.repositories.ingested_file_repository import (
    IngestedFileRepository,
)
from ans_data_platform.repositories.ingestion_run_repository import (
    IngestionRunRepository,
)
from ans_data_platform.storage.postgres_client import PostgresClient


@pytest.fixture
def postgres_connection():
    settings = Settings.from_env()
    postgres = PostgresClient(settings)

    connection = postgres.connect()

    try:
        yield connection
    finally:
        try:
            connection.rollback()
        finally:
            connection.close()


@pytest.fixture
def dataset_id(postgres_connection):
    code = f"file_lifecycle_test_{uuid4().hex}"

    result = postgres_connection.execute(
        """
        INSERT INTO datasets (
            code,
            name,
            description,
            source_url
        )
        VALUES (%s, %s, %s, %s)
        RETURNING id;
        """,
        (
            code,
            "File Lifecycle Integration Test",
            "Temporary dataset for file lifecycle validation.",
            "https://example.invalid/file-lifecycle",
        ),
    ).fetchone()

    return result[0]


@pytest.fixture
def ingestion_run_id(postgres_connection, dataset_id):
    repository = IngestionRunRepository(postgres_connection)

    return repository.create(dataset_id=dataset_id)


@pytest.fixture
def file_repository(postgres_connection):
    return IngestedFileRepository(postgres_connection)


@pytest.fixture
def discovered_file_id(file_repository, ingestion_run_id):
    return file_repository.create(
        ingestion_run_id=ingestion_run_id,
        source_file_name="sample.csv",
        source_url=f"https://example.invalid/{uuid4().hex}/sample.csv",
    )


def test_complete_file_lifecycle(
    file_repository,
    discovered_file_id,
):
    file_id = discovered_file_id

    # DISCOVERED
    discovered = file_repository.get_by_id(file_id)

    assert discovered["status"] == "DISCOVERED"
    assert discovered["downloaded_at"] is None
    assert discovered["stored_at"] is None

    # DOWNLOADED
    content = b"ANS integration test file"
    checksum = hashlib.sha256(content).hexdigest()

    file_repository.mark_downloaded(
        file_id=file_id,
        checksum_sha256=checksum,
        file_size_bytes=len(content),
    )

    downloaded = file_repository.get_by_id(file_id)

    assert downloaded["status"] == "DOWNLOADED"
    assert downloaded["checksum_sha256"] == checksum
    assert downloaded["file_size_bytes"] == len(content)
    assert downloaded["downloaded_at"] is not None

    # STORED
    object_key = "bronze/integration/sample.csv"

    file_repository.mark_stored(
        file_id=file_id,
        object_key=object_key,
    )

    stored = file_repository.get_by_id(file_id)

    assert stored["status"] == "STORED"
    assert stored["object_key"] == object_key
    assert stored["stored_at"] is not None
    assert stored["checksum_sha256"] == checksum

    # Terminal status cannot be changed
    with pytest.raises(ValueError):
        file_repository.mark_failed(
            file_id,
            "Attempt to modify stored file.",
        )


def test_file_failed_from_discovered(
    file_repository,
    discovered_file_id,
):
    file_repository.mark_failed(
        discovered_file_id,
        "Source file unavailable.",
    )

    result = file_repository.get_by_id(discovered_file_id)

    assert result["status"] == "FAILED"
    assert result["error_message"] == "Source file unavailable."


def test_file_failed_after_download(
    file_repository,
    discovered_file_id,
):
    content = b"Downloaded content"
    checksum = hashlib.sha256(content).hexdigest()

    file_repository.mark_downloaded(
        discovered_file_id,
        checksum,
        len(content),
    )

    file_repository.mark_failed(
        discovered_file_id,
        "RustFS upload failed.",
    )

    result = file_repository.get_by_id(discovered_file_id)

    assert result["status"] == "FAILED"
    assert result["error_message"] == "RustFS upload failed."

    # Download metadata must be preserved
    assert result["checksum_sha256"] == checksum
    assert result["file_size_bytes"] == len(content)
    assert result["downloaded_at"] is not None


def test_file_skipped_from_discovered(
    file_repository,
    discovered_file_id,
):
    file_repository.mark_skipped(discovered_file_id)

    result = file_repository.get_by_id(discovered_file_id)

    assert result["status"] == "SKIPPED"
    assert result["downloaded_at"] is None
    assert result["stored_at"] is None


def test_database_rejects_stored_file_without_metadata(
    postgres_connection,
    discovered_file_id,
):
    # Try to bypass the repository and write an invalid state directly.
    with pytest.raises(CheckViolation):
        postgres_connection.execute(
            """
            UPDATE ingested_files
            SET status = 'STORED'
            WHERE id = %s;
            """,
            (discovered_file_id,),
        )
