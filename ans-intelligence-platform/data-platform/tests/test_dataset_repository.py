
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from ans_data_platform.repositories.dataset_repository import (
    DatasetRepository,
)


@pytest.fixture
def mock_connection():
    return MagicMock()


@pytest.fixture
def dataset_repository(mock_connection):
    return DatasetRepository(mock_connection)


def test_create_dataset(dataset_repository, mock_connection):
    # 1. Simulate PostgreSQL returning the generated ID
    mock_connection.execute.return_value.fetchone.return_value = (42,)

    # 2. Create dataset
    dataset_id = dataset_repository.create(
        code="test_dataset",
        name="Test Dataset",
        description="Dataset created during unit testing.",
        source_url="https://example.com/test-dataset",
    )

    # 3. Validate returned ID
    assert dataset_id == 42

    # 4. Validate SQL execution and parameters
    mock_connection.execute.assert_called_once()

    query, parameters = mock_connection.execute.call_args.args

    assert "INSERT INTO datasets" in query
    assert "RETURNING id" in query

    assert parameters == (
        "test_dataset",
        "Test Dataset",
        "Dataset created during unit testing.",
        "https://example.com/test-dataset",
    )


def test_get_dataset_by_code(
    dataset_repository,
    mock_connection,
):
    # 1. Simulate a dataset returned by PostgreSQL
    created_at = datetime(2026, 11, 9, tzinfo=timezone.utc)
    updated_at = datetime(2026, 11, 9, tzinfo=timezone.utc)

    mock_connection.execute.return_value.fetchone.return_value = (
        42,
        "test_dataset",
        "Test Dataset",
        "Dataset created during unit testing.",
        "https://example.com/test-dataset",
        True,
        created_at,
        updated_at,
    )

    # 2. Retrieve dataset
    result = dataset_repository.get_by_code("test_dataset")

    # 3. Validate returned dictionary
    assert result is not None
    assert result["id"] == 42
    assert result["code"] == "test_dataset"
    assert result["name"] == "Test Dataset"
    assert result["description"] == (
        "Dataset created during unit testing."
    )
    assert result["source_url"] == (
        "https://example.com/test-dataset"
    )
    assert result["is_active"] is True
    assert result["created_at"] == created_at
    assert result["updated_at"] == updated_at

    # 4. Validate query parameters
    mock_connection.execute.assert_called_once()

    query, parameters = mock_connection.execute.call_args.args

    assert "SELECT" in query
    assert "FROM datasets" in query
    assert "WHERE code = %s" in query
    assert parameters == ("test_dataset",)


def test_get_dataset_by_code_not_found(
    dataset_repository,
    mock_connection,
):
    # 1. Simulate no dataset found
    mock_connection.execute.return_value.fetchone.return_value = None

    # 2. Retrieve nonexistent dataset
    result = dataset_repository.get_by_code("unknown_dataset")

    # 3. Validate result
    assert result is None

    mock_connection.execute.assert_called_once()

    query, parameters = mock_connection.execute.call_args.args

    assert "WHERE code = %s" in query
    assert parameters == ("unknown_dataset",)
