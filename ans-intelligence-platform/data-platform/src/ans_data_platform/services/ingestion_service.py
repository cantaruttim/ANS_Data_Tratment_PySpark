
import logging
from typing import Any
from uuid import UUID
import hashlib
from pathlib import Path

from ans_data_platform.repositories.dataset_repository import (
    DatasetRepository,
)
from ans_data_platform.repositories.ingestion_run_repository import (
    IngestionRunRepository,
)
from datetime import datetime, timezone
from pathlib import Path

from ans_data_platform.repositories.ingested_file_repository import (
    IngestedFileRepository,
)
from ans_data_platform.storage.postgres_client import PostgresClient
from ans_data_platform.storage.rustfs_client import RustFSClient


logger = logging.getLogger(__name__)


class IngestionService:
    """
    Coordinates ingestion operations between PostgreSQL and RustFS.
    """

    def __init__(
        self,
        postgres: PostgresClient,
        storage: RustFSClient,
    ):
        self.postgres = postgres
        self.storage = storage

    def _get_active_dataset(
        self,
        dataset_code: str,
    ) -> dict[str, Any]:
        """Finds a dataset and verifies whether it is active."""

        with self.postgres.connect() as connection:
            repository = DatasetRepository(connection)
            dataset = repository.get_by_code(dataset_code)

        if dataset is None:
            raise ValueError(
                f"Dataset '{dataset_code}' was not found."
            )

        if not dataset["is_active"]:
            raise ValueError(
                f"Dataset '{dataset_code}' is inactive."
            )

        return dataset

    def _start_ingestion_run(
        self,
        dataset_id: int,
        source_file_name: str,
    ) -> UUID:
        """Creates and starts an ingestion execution."""

        parameters = {
            "source_type": "local_file",
            "source_file_name": source_file_name,
        }

        # Both SQL operations share the same PostgreSQL transaction.
        with self.postgres.connect() as connection:
            repository = IngestionRunRepository(connection)

            run_id = repository.create(
                dataset_id=dataset_id,
                trigger_type="MANUAL",
                parameters=parameters,
            )

            repository.mark_running(run_id)

        logger.info(
            "Ingestion execution started: %s",
            run_id,
        )

        return run_id


    @staticmethod
    def _calculate_file_metadata(
        file_path: str | Path,
        chunk_size_bytes: int = 8 * 1024 * 1024,
    ) -> tuple[str, int]:
        """
        Calculates the SHA-256 checksum and size of a file
        without loading the entire file into memory.
        """

        source = Path(file_path)

        if not source.is_file():
            raise FileNotFoundError(
                f"File not found: {source}"
            )

        if chunk_size_bytes <= 0:
            raise ValueError(
                "Chunk size must be greater than zero."
            )

        checksum = hashlib.sha256()
        file_size_bytes = 0

        with source.open("rb") as file:
            while True:
                chunk = file.read(chunk_size_bytes)

                if not chunk:
                    break

                checksum.update(chunk)
                file_size_bytes += len(chunk)

        checksum_sha256 = checksum.hexdigest()

        logger.debug(
            "File metadata calculated: %s (%d bytes)",
            source.name,
            file_size_bytes,
        )

        return checksum_sha256, file_size_bytes


    def _register_downloaded_file(
        self,
        ingestion_run_id: UUID,
        file_path: str | Path,
        source_url: str | None = None,
    ) -> int:
        """
        Registers a discovered file and updates its status
        after calculating its metadata.
        """

        source = Path(file_path)

        if not source.is_file():
            raise FileNotFoundError(
                f"File not found: {source}"
            )

        resolved_source = source.resolve()

        # 1. Identify source metadata
        source_stat = resolved_source.stat()

        resolved_source_url = (
            source_url
            if source_url is not None
            else resolved_source.as_uri()
        )

        if not resolved_source_url.strip():
            raise ValueError("Source URL cannot be empty.")

        source_modified_at = datetime.fromtimestamp(
            source_stat.st_mtime,
            tz=timezone.utc,
        )

        # 2. Register file as DISCOVERED
        with self.postgres.connect() as connection:
            repository = IngestedFileRepository(connection)

            file_id = repository.create(
                ingestion_run_id=ingestion_run_id,
                source_file_name=resolved_source.name,
                source_url=resolved_source_url,
                source_modified_at=source_modified_at,
            )

        logger.info(
            "File registered as DISCOVERED: %s",
            resolved_source.name,
        )

        # 3. Calculate file metadata outside the DB transaction
        checksum_sha256, file_size_bytes = (
            self._calculate_file_metadata(resolved_source)
        )

        # 4. Update file status to DOWNLOADED
        with self.postgres.connect() as connection:
            repository = IngestedFileRepository(connection)

            repository.mark_downloaded(
                file_id=file_id,
                checksum_sha256=checksum_sha256,
                file_size_bytes=file_size_bytes,
            )

        logger.info(
            "File registered as DOWNLOADED: %s",
            resolved_source.name,
        )

        return file_id
