
import logging
from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg import Connection


logger = logging.getLogger(__name__)


class IngestedFileRepository:
    def __init__(self, connection: Connection):
        self.connection = connection

    def create(
        self,
        ingestion_run_id: UUID,
        source_file_name: str,
        source_url: str,
        source_modified_at: datetime | None = None,
    ) -> int:

        query = """
            INSERT INTO ingested_files (
                ingestion_run_id,
                source_file_name,
                source_url,
                source_modified_at
            )
            VALUES (%s, %s, %s, %s)
            RETURNING id;
        """

        result = self.connection.execute(
            query,
            (
                ingestion_run_id,
                source_file_name,
                source_url,
                source_modified_at,
            ),
        ).fetchone()

        if result is None:
            raise RuntimeError(
                "Ingested file creation returned no ID."
            )

        file_id = result[0]

        logger.info(
            "Ingested file discovered: %s (ID: %s)",
            source_file_name,
            file_id,
        )

        return file_id

    def get_by_id(
        self,
        file_id: int,
    ) -> dict[str, Any] | None:

        query = """
            SELECT
                id,
                ingestion_run_id,
                source_file_name,
                source_url,
                object_key,
                status,
                checksum_sha256,
                file_size_bytes,
                source_modified_at,
                downloaded_at,
                stored_at,
                error_message,
                created_at
            FROM ingested_files
            WHERE id = %s;
        """

        result = self.connection.execute(
            query,
            (file_id,),
        ).fetchone()

        if result is None:
            logger.debug("Ingested file not found: %s", file_id)
            return None

        columns = [
            "id",
            "ingestion_run_id",
            "source_file_name",
            "source_url",
            "object_key",
            "status",
            "checksum_sha256",
            "file_size_bytes",
            "source_modified_at",
            "downloaded_at",
            "stored_at",
            "error_message",
            "created_at",
        ]

        return dict(zip(columns, result))

    def list_by_run_id(
        self,
        ingestion_run_id: UUID,
    ) -> list[dict[str, Any]]:

        query = """
            SELECT
                id,
                ingestion_run_id,
                source_file_name,
                source_url,
                object_key,
                status,
                checksum_sha256,
                file_size_bytes,
                source_modified_at,
                downloaded_at,
                stored_at,
                error_message,
                created_at
            FROM ingested_files
            WHERE ingestion_run_id = %s
            ORDER BY id;
        """

        results = self.connection.execute(
            query,
            (ingestion_run_id,),
        ).fetchall()

        columns = [
            "id",
            "ingestion_run_id",
            "source_file_name",
            "source_url",
            "object_key",
            "status",
            "checksum_sha256",
            "file_size_bytes",
            "source_modified_at",
            "downloaded_at",
            "stored_at",
            "error_message",
            "created_at",
        ]

        return [
            dict(zip(columns, row))
            for row in results
        ]
