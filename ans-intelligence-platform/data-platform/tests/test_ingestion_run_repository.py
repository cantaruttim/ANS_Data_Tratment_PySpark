
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from psycopg.types.json import Jsonb

from ans_data_platform.repositories.ingestion_run_repository import (
    IngestionRunRepository,
)


@pytest.fixture
def mock_connection():
    return MagicMock()


@pytest.fixture
def ingestion_run_repository(mock_connection):
    return IngestionRunRepository(mock_connection)


def test_create_ingestion_run(
    ingestion_run_repository,
    mock_connection,
):
    # 1. Simulate PostgreSQL returning a UUID
    expected_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    mock_connection.execute.return_value.fetchone.return_value = (
        expected_id,
    )

    parameters = {
        "source": "ANS_FTP",
        "year": 2025,
        "format": "csv",
    }

    # 2. Create ingestion run
    run_id = ingestion_run_repository.create(
        dataset_id=1,
        trigger_type="SCHEDULED",
        parameters=parameters,
    )

    # 3. Validate returned UUID
    assert run_id == expected_id
    assert isinstance(run_id, UUID)

    # 4. Validate SQL and parameters
    mock_connection.execute.assert_called_once()

    query, values = mock_connection.execute.call_args.args

    assert "INSERT INTO ingestion_runs" in query
    assert "RETURNING id" in query

    assert values[0] == 1
    assert values[1] == "SCHEDULED"
    assert isinstance(values[2], Jsonb)
    assert values[2].obj == parameters


def test_create_ingestion_run_with_default_parameters(
    ingestion_run_repository,
    mock_connection,
):
    expected_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    mock_connection.execute.return_value.fetchone.return_value = (
        expected_id,
    )

    run_id = ingestion_run_repository.create(dataset_id=1)

    assert run_id == expected_id

    _, values = mock_connection.execute.call_args.args

    assert values[0] == 1
    assert values[1] == "MANUAL"
    assert isinstance(values[2], Jsonb)
    assert values[2].obj == {}


def test_create_ingestion_run_without_returned_id(
    ingestion_run_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = None

    with pytest.raises(
        RuntimeError,
        match="Ingestion run creation returned no ID",
    ):
        ingestion_run_repository.create(dataset_id=1)


def test_get_ingestion_run_by_id(
    ingestion_run_repository,
    mock_connection,
):
    # 1. Simulate PostgreSQL returning an ingestion run
    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    created_at = datetime(
        2026, 11, 9, tzinfo=timezone.utc
    )

    parameters = {
        "source": "ANS_FTP",
        "year": 2025,
        "format": "csv",
    }

    mock_connection.execute.return_value.fetchone.return_value = (
        run_id,
        1,
        "PENDING",
        "MANUAL",
        None,
        None,
        parameters,
        None,
        created_at,
    )

    # 2. Retrieve ingestion run
    result = ingestion_run_repository.get_by_id(run_id)

    # 3. Validate returned dictionary
    assert result is not None
    assert result["id"] == run_id
    assert result["dataset_id"] == 1
    assert result["status"] == "PENDING"
    assert result["trigger_type"] == "MANUAL"
    assert result["started_at"] is None
    assert result["finished_at"] is None
    assert result["parameters"] == parameters
    assert result["error_message"] is None
    assert result["created_at"] == created_at

    # 4. Validate query parameters
    query, values = mock_connection.execute.call_args.args

    assert "FROM ingestion_runs" in query
    assert "WHERE id = %s" in query
    assert values == (run_id,)


def test_get_ingestion_run_by_id_not_found(
    ingestion_run_repository,
    mock_connection,
):
    mock_connection.execute.return_value.fetchone.return_value = None

    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    result = ingestion_run_repository.get_by_id(run_id)

    assert result is None

    query, values = mock_connection.execute.call_args.args

    assert "WHERE id = %s" in query
    assert values == (run_id,)



def test_mark_running(
    ingestion_run_repository,
    mock_connection,
):
    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    mock_connection.execute.return_value.fetchone.return_value = (
        run_id,
    )

    ingestion_run_repository.mark_running(run_id)

    mock_connection.execute.assert_called_once()

    query, parameters = mock_connection.execute.call_args.args

    assert "UPDATE ingestion_runs" in query
    assert "status = 'RUNNING'" in query
    assert "started_at = CURRENT_TIMESTAMP" in query
    assert "status = 'PENDING'" in query

    assert parameters == (run_id,)


def test_mark_running_invalid_transition(
    ingestion_run_repository,
    mock_connection,
):
    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    mock_connection.execute.return_value.fetchone.return_value = None

    with pytest.raises(
        ValueError,
        match="was not found or is not in PENDING status",
    ):
        ingestion_run_repository.mark_running(run_id)


def test_mark_succeeded(
    ingestion_run_repository,
    mock_connection,
):
    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    mock_connection.execute.return_value.fetchone.return_value = (
        run_id,
    )

    ingestion_run_repository.mark_succeeded(run_id)

    query, values = mock_connection.execute.call_args.args

    assert "UPDATE ingestion_runs" in query
    assert "finished_at = CURRENT_TIMESTAMP" in query
    assert "status = 'RUNNING'" in query
    assert values == ("SUCCEEDED", None, run_id)


def test_mark_partial(
    ingestion_run_repository,
    mock_connection,
):
    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")
    error_message = "Some files failed during ingestion."

    mock_connection.execute.return_value.fetchone.return_value = (
        run_id,
    )

    ingestion_run_repository.mark_partial(
        run_id,
        error_message,
    )

    _, values = mock_connection.execute.call_args.args

    assert values == ("PARTIAL", error_message, run_id)


def test_mark_failed(
    ingestion_run_repository,
    mock_connection,
):
    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")
    error_message = "FTP connection failed."

    mock_connection.execute.return_value.fetchone.return_value = (
        run_id,
    )

    ingestion_run_repository.mark_failed(
        run_id,
        error_message,
    )

    _, values = mock_connection.execute.call_args.args

    assert values == ("FAILED", error_message, run_id)


def test_mark_cancelled(
    ingestion_run_repository,
    mock_connection,
):
    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")
    reason = "Execution cancelled by user."

    mock_connection.execute.return_value.fetchone.return_value = (
        run_id,
    )

    ingestion_run_repository.mark_cancelled(run_id, reason)

    query, values = mock_connection.execute.call_args.args

    assert "UPDATE ingestion_runs" in query
    assert "'PENDING', 'RUNNING'" in query
    assert values == (reason, run_id)


def test_mark_succeeded_invalid_transition(
    ingestion_run_repository,
    mock_connection,
):
    run_id = UUID("c12861bd-496f-4cdc-ac54-da5b8fa10c24")

    mock_connection.execute.return_value.fetchone.return_value = None

    with pytest.raises(
        ValueError,
        match="was not found or is not in RUNNING status",
    ):
        ingestion_run_repository.mark_succeeded(run_id)
