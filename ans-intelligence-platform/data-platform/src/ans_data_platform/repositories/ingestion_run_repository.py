
import logging
from typing import Any
from uuid import UUID

from psycopg import Connection
from psycopg.types.json import Jsonb


logger = logging.getLogger(__name__)


class IngestionRunRepository:
    def __init__(self, connection: Connection):
        self.connection = connection

    def create(
        self,
        dataset_id: int,
        trigger_type: str = "MANUAL",
        parameters: dict[str, Any] | None = None,
    ) -> UUID:

        query = """
            INSERT INTO ingestion_runs (
                dataset_id,
                trigger_type,
                parameters
            )
            VALUES (%s, %s, %s)
            RETURNING id;
        """

        result = self.connection.execute(
            query,
            (
                dataset_id,
                trigger_type,
                Jsonb(parameters if parameters is not None else {}),
            ),
        ).fetchone()

        if result is None:
            raise RuntimeError(
                "Ingestion run creation returned no ID."
            )

        run_id = result[0]

        logger.info(
            "Ingestion run created: %s (Dataset ID: %s)",
            run_id,
            dataset_id,
        )

        return run_id

    def get_by_id(
        self,
        run_id: UUID,
    ) -> dict[str, Any] | None:

        query = """
            SELECT
                id,
                dataset_id,
                status,
                trigger_type,
                started_at,
                finished_at,
                parameters,
                error_message,
                created_at
            FROM ingestion_runs
            WHERE id = %s;
        """

        result = self.connection.execute(
            query,
            (run_id,),
        ).fetchone()

        if result is None:
            logger.debug("Ingestion run not found: %s", run_id)
            return None

        columns = [
            "id",
            "dataset_id",
            "status",
            "trigger_type",
            "started_at",
            "finished_at",
            "parameters",
            "error_message",
            "created_at",
        ]

        return dict(zip(columns, result))

    
    def mark_running(self, run_id: UUID) -> None:
        query = """
            UPDATE ingestion_runs
            SET
                status = 'RUNNING',
                started_at = CURRENT_TIMESTAMP
            WHERE id = %s
              AND status = 'PENDING'
            RETURNING id;
        """

        result = self.connection.execute(
            query,
            (run_id,),
        ).fetchone()

        if result is None:
            raise ValueError(
                f"Ingestion run '{run_id}' was not found "
                "or is not in PENDING status."
            )

        logger.info(
            "Ingestion run started: %s",
            run_id,
        )

    
    def _finish_running(
        self,
        run_id: UUID,
        status: str,
        error_message: str | None = None,
    ) -> None:

        query = """
            UPDATE ingestion_runs
            SET
                status = %s,
                finished_at = CURRENT_TIMESTAMP,
                error_message = %s
            WHERE id = %s
              AND status = 'RUNNING'
            RETURNING id;
        """

        result = self.connection.execute(
            query,
            (status, error_message, run_id),
        ).fetchone()

        if result is None:
            raise ValueError(
                f"Ingestion run '{run_id}' was not found "
                "or is not in RUNNING status."
            )

        logger.info(
            "Ingestion run finished: %s (Status: %s)",
            run_id,
            status,
        )

    def mark_succeeded(self, run_id: UUID) -> None:
        self._finish_running(
            run_id=run_id,
            status="SUCCEEDED",
        )

    def mark_partial(
        self,
        run_id: UUID,
        error_message: str | None = None,
    ) -> None:
        self._finish_running(
            run_id=run_id,
            status="PARTIAL",
            error_message=error_message,
        )

    def mark_failed(
        self,
        run_id: UUID,
        error_message: str,
    ) -> None:
        self._finish_running(
            run_id=run_id,
            status="FAILED",
            error_message=error_message,
        )

    def mark_cancelled(
        self,
        run_id: UUID,
        reason: str | None = None,
    ) -> None:

        query = """
            UPDATE ingestion_runs
            SET
                status = 'CANCELLED',
                finished_at = CURRENT_TIMESTAMP,
                error_message = %s
            WHERE id = %s
              AND status IN ('PENDING', 'RUNNING')
            RETURNING id;
        """

        result = self.connection.execute(
            query,
            (reason, run_id),
        ).fetchone()

        if result is None:
            raise ValueError(
                f"Ingestion run '{run_id}' was not found "
                "or cannot be cancelled from its current status."
            )

        logger.info(
            "Ingestion run cancelled: %s",
            run_id,
        )


