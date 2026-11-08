
import pytest

from ans_data_platform.config.settings import Settings
from ans_data_platform.storage.postgres_client import PostgresClient


@pytest.mark.integration
def test_postgres_connection_and_query():
    # 1. Load application settings
    settings = Settings.from_env()

    # 2. Initialize PostgreSQL client
    postgres = PostgresClient(settings)

    # 3. Establish connection and execute query
    with postgres.connect() as connection:
        result = connection.execute(
            """
            SELECT
                current_database(),
                current_user,
                version();
            """
        ).fetchone()

    # 4. Validate query results
    assert result is not None

    database_name = result[0]
    username = result[1]
    database_version = result[2]

    assert database_name == settings.postgres_db
    assert username == settings.postgres_user
    assert "PostgreSQL" in database_version
