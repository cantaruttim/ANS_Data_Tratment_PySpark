
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



    def mark_downloaded(
        self,
        file_id: int,
        checksum_sha256: str,
        file_size_bytes: int,
    ) -> None:

        query = """
            UPDATE ingested_files
            SET
                status = 'DOWNLOADED',
                checksum_sha256 = %s,
                file_size_bytes = %s,
                downloaded_at = CURRENT_TIMESTAMP
            WHERE id = %s
              AND status = 'DISCOVERED'
            RETURNING id;
        """

        result = self.connection.execute(
            query,
            (
                checksum_sha256,
                file_size_bytes,
                file_id,
            ),
        ).fetchone()

        if result is None:
            raise ValueError(
                f"File '{file_id}' was not found "
                "or is not in DISCOVERED status."
            )

        logger.info(
            "Ingested file downloaded: %s",
            file_id,
        )



    def mark_stored(
        self,
        file_id: int,
        object_key: str,
    ) -> None:

        query = """
            UPDATE ingested_files
            SET
                status = 'STORED',
                object_key = %s,
                stored_at = CURRENT_TIMESTAMP
            WHERE id = %s
              AND status = 'DOWNLOADED'
            RETURNING id;
        """

        result = self.connection.execute(
            query,
            (
                object_key,
                file_id,
            ),
        ).fetchone()

        if result is None:
            raise ValueError(
                f"File '{file_id}' was not found "
                "or is not in DOWNLOADED status."
            )

        logger.info(
            "Ingested file stored: %s (Object key: %s)",
            file_id,
            object_key,
        )


    def mark_failed(
        self,
        file_id: int,
        error_message: str,
    ) -> None:

        query = """
            UPDATE ingested_files
            SET
                status = 'FAILED',
                error_message = %s
            WHERE id = %s
              AND status IN ('DISCOVERED', 'DOWNLOADED')
            RETURNING id;
        """

        result = self.connection.execute(
            query,
            (error_message, file_id),
        ).fetchone()

        if result is None:
            raise ValueError(
                f"File '{file_id}' was not found "
                "or cannot be marked as FAILED."
            )

        logger.warning(
            "Ingested file failed: %s (%s)",
            file_id,
            error_message,
        )

    def mark_skipped(self, file_id: int) -> None:
        query = """
            UPDATE ingested_files
            SET status = 'SKIPPED'
            WHERE id = %s
              AND status = 'DISCOVERED'
            RETURNING id;
        """

        result = self.connection.execute(
            query,
            (file_id,),
        ).fetchone()

        if result is None:
            raise ValueError(
                f"File '{file_id}' was not found "
                "or cannot be marked as SKIPPED."
            )

        logger.info(
            "Ingested file skipped: %s",
            file_id,
        )
