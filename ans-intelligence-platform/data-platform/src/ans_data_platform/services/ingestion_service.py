
import logging

from ans_data_platform.storage.postgres_client import PostgresClient
from ans_data_platform.storage.rustfs_client import RustFSClient


logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(
        self,
        postgres: PostgresClient,
        storage: RustFSClient,
    ):
        self.postgres = postgres
        self.storage = storage
