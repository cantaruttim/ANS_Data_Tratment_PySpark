
import logging

import psycopg
from psycopg import Connection

from ans_data_platform.config.settings import Settings


logger = logging.getLogger(__name__)


class PostgresClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    def connect(self) -> Connection:
        logger.info(
            "Connecting to PostgreSQL at %s:%s/%s",
            self.settings.postgres_host,
            self.settings.postgres_port,
            self.settings.postgres_db,
        )

        connection = psycopg.connect(
            host=self.settings.postgres_host,
            port=self.settings.postgres_port,
            dbname=self.settings.postgres_db,
            user=self.settings.postgres_user,
            password=self.settings.postgres_password,
        )

        logger.info("PostgreSQL connection established")

        return connection
