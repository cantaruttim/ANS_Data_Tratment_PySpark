
from unittest.mock import MagicMock, patch

import pytest

from ans_data_platform.config.settings import Settings
from ans_data_platform.storage.postgres_client import PostgresClient


@pytest.fixture
def settings():
    return Settings(
        # RustFS
        s3_endpoint_url="http://localhost:9000",
        s3_bucket_name="ans-data-lake",
        s3_region="us-east-1",
        s3_access_key="test-access-key",
        s3_secret_key="test-secret-key",

        # PostgreSQL
        postgres_host="localhost",
        postgres_port=5434,
        postgres_db="ans_intelligence",
        postgres_user="test-user",
        postgres_password="test-password",
    )


def test_connect_uses_settings(settings):
    expected_connection = MagicMock()

    with patch(
        "ans_data_platform.storage.postgres_client.psycopg.connect"
    ) as mock_connect:

        mock_connect.return_value = expected_connection

        postgres = PostgresClient(settings)

        connection = postgres.connect()

        mock_connect.assert_called_once_with(
            host="localhost",
            port=5434,
            dbname="ans_intelligence",
            user="test-user",
            password="test-password",
        )

        assert connection is expected_connection
