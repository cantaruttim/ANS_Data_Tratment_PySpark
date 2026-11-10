
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
