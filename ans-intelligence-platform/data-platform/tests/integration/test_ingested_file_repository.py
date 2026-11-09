
from datetime import datetime, timezone
from uuid import uuid4

import pytest

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
    unique_code = f"integration_file_test_{uuid4().hex}"

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
            unique_code,
            "Ingested File Integration Test",
            "Temporary dataset for repository integration tests.",
            "https://example.invalid/integration-test",
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


def test_create_and_get_ingested_file(
    file_repository,
    ingestion_run_id,
):
    source_modified_at = datetime(
        2026, 11, 9, tzinfo=timezone.utc
    )

    file_id = file_repository.create(
        ingestion_run_id=ingestion_run_id,
        source_file_name="demonstracoes_2025.csv",
        source_url="https://example.invalid/demonstracoes_2025.csv",
        source_modified_at=source_modified_at,
    )

    result = file_repository.get_by_id(file_id)

    assert result is not None
    assert result["id"] == file_id
    assert result["ingestion_run_id"] == ingestion_run_id
    assert result["source_file_name"] == "demonstracoes_2025.csv"
    assert result["status"] == "DISCOVERED"
    assert result["object_key"] is None
    assert result["checksum_sha256"] is None
    assert result["source_modified_at"] == source_modified_at


def test_get_ingested_file_not_found(file_repository):
    result = file_repository.get_by_id(999999999)

    assert result is None


def test_list_ingested_files_by_run_id(
    file_repository,
    ingestion_run_id,
):
    file_repository.create(
        ingestion_run_id=ingestion_run_id,
        source_file_name="demonstracoes_2024.csv",
        source_url="https://example.invalid/demonstracoes_2024.csv",
    )

    file_repository.create(
        ingestion_run_id=ingestion_run_id,
        source_file_name="demonstracoes_2025.csv",
        source_url="https://example.invalid/demonstracoes_2025.csv",
    )

    result = file_repository.list_by_run_id(ingestion_run_id)

    assert len(result) == 2

    assert result[0]["source_file_name"] == "demonstracoes_2024.csv"
    assert result[1]["source_file_name"] == "demonstracoes_2025.csv"

    assert all(
        item["status"] == "DISCOVERED"
        for item in result
    )


def test_list_ingested_files_by_run_id_empty(
    file_repository,
    ingestion_run_id,
):
    result = file_repository.list_by_run_id(ingestion_run_id)

    assert result == []


def test_mark_downloaded(
    ingested_file_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = (10,)

    checksum = (
        "79d58b38633c27f9ba0a0eab34ba8a0b"
        "68d24f8789fceae2429162ca4c4c9c38"
    )

    ingested_file_repository.mark_downloaded(
        file_id=10,
        checksum_sha256=checksum,
        file_size_bytes=46,
    )

    query, values = mock_connection.execute.call_args.args

    assert "status = 'DOWNLOADED'" in query
    assert "downloaded_at = CURRENT_TIMESTAMP" in query
    assert "status = 'DISCOVERED'" in query

    assert values == (checksum, 46, 10)


def test_mark_downloaded_invalid_transition(
    ingested_file_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = None

    with pytest.raises(
        ValueError,
        match="was not found or is not in DISCOVERED status",
    ):
        ingested_file_repository.mark_downloaded(
            file_id=10,
            checksum_sha256="a" * 64,
            file_size_bytes=46,
        )


def test_mark_stored(
    ingested_file_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = (10,)

    object_key = "bronze/test/file.csv"

    ingested_file_repository.mark_stored(
        file_id=10,
        object_key=object_key,
    )

    query, values = mock_connection.execute.call_args.args

    assert "status = 'STORED'" in query
    assert "stored_at = CURRENT_TIMESTAMP" in query
    assert "status = 'DOWNLOADED'" in query

    assert values == (object_key, 10)


def test_mark_stored_invalid_transition(
    ingested_file_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = None

    with pytest.raises(
        ValueError,
        match="was not found or is not in DOWNLOADED status",
    ):
        ingested_file_repository.mark_stored(
            file_id=10,
            object_key="bronze/test/file.csv",
        )
