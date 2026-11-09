
from uuid import uuid4

import pytest

from ans_data_platform.config.settings import Settings
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
    code = f"integration_test_{uuid4().hex}"

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
            "Integration Test Dataset",
            "Temporary dataset for integration tests.",
            "https://example.invalid/integration-test",
        ),
    ).fetchone()

    return result[0]


def test_successful_ingestion_lifecycle(
    postgres_connection,
    dataset_id,
):
    repository = IngestionRunRepository(postgres_connection)

    # Create execution
    run_id = repository.create(
        dataset_id=dataset_id,
        parameters={"source": "integration_test"},
    )

    created = repository.get_by_id(run_id)

    assert created["status"] == "PENDING"
    assert created["parameters"] == {"source": "integration_test"}

    # Start execution
    repository.mark_running(run_id)

    running = repository.get_by_id(run_id)

    assert running["status"] == "RUNNING"
    assert running["started_at"] is not None
    assert running["finished_at"] is None

    # Complete execution
    repository.mark_succeeded(run_id)

    completed = repository.get_by_id(run_id)

    assert completed["status"] == "SUCCEEDED"
    assert completed["started_at"] is not None
    assert completed["finished_at"] is not None
    assert completed["error_message"] is None

    # Terminal executions cannot be restarted
    with pytest.raises(ValueError):
        repository.mark_running(run_id)


def test_cancel_pending_ingestion(
    postgres_connection,
    dataset_id,
):
    repository = IngestionRunRepository(postgres_connection)

    run_id = repository.create(dataset_id=dataset_id)

    repository.mark_cancelled(
        run_id,
        reason="Cancelled before execution.",
    )

    cancelled = repository.get_by_id(run_id)

    assert cancelled["status"] == "CANCELLED"
    assert cancelled["started_at"] is None
    assert cancelled["finished_at"] is not None
    assert cancelled["error_message"] == (
        "Cancelled before execution."
    )


def test_partial_ingestion(
    postgres_connection,
    dataset_id,
):
    repository = IngestionRunRepository(postgres_connection)

    run_id = repository.create(dataset_id=dataset_id)

    repository.mark_running(run_id)

    repository.mark_partial(
        run_id,
        error_message="Some files could not be processed.",
    )

    result = repository.get_by_id(run_id)

    assert result["status"] == "PARTIAL"
    assert result["started_at"] is not None
    assert result["finished_at"] is not None
    assert result["error_message"] == (
        "Some files could not be processed."
    )


def test_failed_ingestion(
    postgres_connection,
    dataset_id,
):
    repository = IngestionRunRepository(postgres_connection)

    run_id = repository.create(dataset_id=dataset_id)

    repository.mark_running(run_id)

    repository.mark_failed(
        run_id,
        error_message="FTP connection failed.",
    )

    result = repository.get_by_id(run_id)

    assert result["status"] == "FAILED"
    assert result["started_at"] is not None
    assert result["finished_at"] is not None
    assert result["error_message"] == "FTP connection failed."
