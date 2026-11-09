
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from ans_data_platform.repositories.ingested_file_repository import (
    IngestedFileRepository,
)


@pytest.fixture
def mock_connection():
    return MagicMock()


@pytest.fixture
def ingested_file_repository(mock_connection):
    return IngestedFileRepository(mock_connection)


def test_create_ingested_file(
    ingested_file_repository,
    mock_connection,
):
    # 1. Simulate PostgreSQL returning the generated ID
    mock_connection.execute.return_value.fetchone.return_value = (10,)

    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    source_modified_at = datetime(
        2026, 11, 9, tzinfo=timezone.utc
    )

    # 2. Register file
    file_id = ingested_file_repository.create(
        ingestion_run_id=run_id,
        source_file_name="demonstracoes_2025.csv",
        source_url="https://example.com/demonstracoes_2025.csv",
        source_modified_at=source_modified_at,
    )

    # 3. Validate returned ID
    assert file_id == 10

    # 4. Validate SQL and parameters
    query, values = mock_connection.execute.call_args.args

    assert "INSERT INTO ingested_files" in query
    assert "RETURNING id" in query

    assert values == (
        run_id,
        "demonstracoes_2025.csv",
        "https://example.com/demonstracoes_2025.csv",
        source_modified_at,
    )


def test_create_ingested_file_without_returned_id(
    ingested_file_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = None

    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    with pytest.raises(
        RuntimeError,
        match="Ingested file creation returned no ID",
    ):
        ingested_file_repository.create(
            ingestion_run_id=run_id,
            source_file_name="test.csv",
            source_url="https://example.com/test.csv",
        )


def test_get_ingested_file_by_id(
    ingested_file_repository,
    mock_connection,
):
    # 1. Simulate PostgreSQL returning a file
    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    created_at = datetime(
        2026, 11, 9, tzinfo=timezone.utc
    )

    mock_connection.execute.return_value.fetchone.return_value = (
        10,
        run_id,
        "demonstracoes_2025.csv",
        "https://example.com/demonstracoes_2025.csv",
        "bronze/demonstracoes_2025.csv",
        "STORED",
        "79d58b38633c27f9ba0a0eab34ba8a0b68d24f8789fceae2429162ca4c4c9c38",
        1024,
        None,
        created_at,
        created_at,
        None,
        created_at,
    )

    # 2. Retrieve file
    result = ingested_file_repository.get_by_id(10)

    # 3. Validate returned dictionary
    assert result is not None
    assert result["id"] == 10
    assert result["ingestion_run_id"] == run_id
    assert result["source_file_name"] == "demonstracoes_2025.csv"
    assert result["status"] == "STORED"
    assert result["object_key"] == "bronze/demonstracoes_2025.csv"
    assert result["file_size_bytes"] == 1024

    # 4. Validate query parameters
    query, values = mock_connection.execute.call_args.args

    assert "FROM ingested_files" in query
    assert "WHERE id = %s" in query
    assert values == (10,)


def test_get_ingested_file_by_id_not_found(
    ingested_file_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = None

    result = ingested_file_repository.get_by_id(999)

    assert result is None

    query, values = mock_connection.execute.call_args.args

    assert "WHERE id = %s" in query
    assert values == (999,)


def test_list_ingested_files_by_run_id(
    ingested_file_repository,
    mock_connection,
):
    # 1. Simulate two files returned by PostgreSQL
    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    created_at = datetime(
        2026, 11, 9, tzinfo=timezone.utc
    )

    first_file = (
        10,
        run_id,
        "demonstracoes_2024.csv",
        "https://example.com/demonstracoes_2024.csv",
        "bronze/demonstracoes_2024.csv",
        "STORED",
        None,
        1024,
        None,
        created_at,
        created_at,
        None,
        created_at,
    )

    second_file = (
        11,
        run_id,
        "demonstracoes_2025.csv",
        "https://example.com/demonstracoes_2025.csv",
        "bronze/demonstracoes_2025.csv",
        "STORED",
        None,
        2048,
        None,
        created_at,
        created_at,
        None,
        created_at,
    )

    mock_connection.execute.return_value.fetchall.return_value = [
        first_file,
        second_file,
    ]

    # 2. Retrieve files
    result = ingested_file_repository.list_by_run_id(run_id)

    # 3. Validate result
    assert len(result) == 2

    assert result[0]["id"] == 10
    assert result[0]["source_file_name"] == "demonstracoes_2024.csv"

    assert result[1]["id"] == 11
    assert result[1]["source_file_name"] == "demonstracoes_2025.csv"

    # 4. Validate query
    query, values = mock_connection.execute.call_args.args

    assert "FROM ingested_files" in query
    assert "WHERE ingestion_run_id = %s" in query
    assert "ORDER BY id" in query
    assert values == (run_id,)


def test_list_ingested_files_by_run_id_empty(
    ingested_file_repository,
    mock_connection,
):
    # Simulate an execution without registered files
    mock_connection.execute.return_value.fetchall.return_value = []

    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    result = ingested_file_repository.list_by_run_id(run_id)

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


def test_mark_failed(
    ingested_file_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = (10,)

    error_message = "RustFS upload failed."

    ingested_file_repository.mark_failed(
        file_id=10,
        error_message=error_message,
    )

    query, values = mock_connection.execute.call_args.args

    assert "status = 'FAILED'" in query
    assert "DISCOVERED', 'DOWNLOADED'" in query
    assert values == (error_message, 10)


def test_mark_failed_invalid_transition(
    ingested_file_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = None

    with pytest.raises(
        ValueError,
        match="cannot be marked as FAILED",
    ):
        ingested_file_repository.mark_failed(
            file_id=10,
            error_message="Upload failed.",
        )


def test_mark_skipped(
    ingested_file_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = (10,)

    ingested_file_repository.mark_skipped(file_id=10)

    query, values = mock_connection.execute.call_args.args

    assert "status = 'SKIPPED'" in query
    assert "status = 'DISCOVERED'" in query
    assert values == (10,)


def test_mark_skipped_invalid_transition(
    ingested_file_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = None

    with pytest.raises(
        ValueError,
        match="cannot be marked as SKIPPED",
    ):
        ingested_file_repository.mark_skipped(file_id=10)
