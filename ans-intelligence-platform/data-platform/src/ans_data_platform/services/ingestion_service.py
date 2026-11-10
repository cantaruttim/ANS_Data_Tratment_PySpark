
import logging
from typing import Any
from uuid import UUID

from pathlib import Path

from ans_data_platform.repositories.dataset_repository import (
    DatasetRepository,
)
from ans_data_platform.repositories.ingestion_run_repository import (
    IngestionRunRepository,
)
from ans_data_platform.repositories.ingested_file_repository import (
    IngestedFileRepository,
)

from ans_data_platform.storage.postgres_client import PostgresClient
from ans_data_platform.storage.rustfs_client import RustFSClient
logger = logging.getLogger(__name__)


class IngestionService:
    """
        Ingestão de dependência. 
        Será usado para orquestrar toda a parte da infra (+ teste)
    """
    def __init__(self, postgres: PostgresClient, storage: RustFSClient):
        self.postgres = postgres
        self.storage = storage

    "Localiza o dataset e verifica se ele está ativo"
    def _get_active_dataset(self, dataset_code: str) -> dict[str, Any]:

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

        parameters = {
            "source_type": "local_file",
            "source_file_name": source_file_name,
        }

        # Duas transações pertencem a mesma transação
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
