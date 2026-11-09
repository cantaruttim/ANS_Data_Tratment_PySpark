
import logging
from typing import Any

from psycopg import Connection


logger = logging.getLogger(__name__)


class DatasetRepository:
    def __init__(self, connection: Connection):
        self.connection = connection

    def create(
        self,
        code: str,
        name: str,
        description: str | None,
        source_url: str,
    ) -> int:

        query = """
            INSERT INTO datasets (
                code,
                name,
                description,
                source_url
            )
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """

        result = self.connection.execute(
            query,
            (code, name, description, source_url),
        ).fetchone()

        if result is None:
            raise RuntimeError("Dataset creation returned no ID.")

        dataset_id = result[0]

        logger.info(
            "Dataset created: %s (ID: %s)",
            code,
            dataset_id,
        )

        return dataset_id

    def get_by_code(self, code: str) -> dict[str, Any] | None:

        query = """
            SELECT
                id,
                code,
                name,
                description,
                source_url,
                is_active,
                created_at,
                updated_at
            FROM datasets
            WHERE code = %s;
        """

        result = self.connection.execute(
            query,
            (code,),
        ).fetchone()

        if result is None:
            logger.debug("Dataset not found: %s", code)
            return None

        columns = [
            "id",
            "code",
            "name",
            "description",
            "source_url",
            "is_active",
            "created_at",
            "updated_at",
        ]

        return dict(zip(columns, result))
