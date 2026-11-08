import logging
import os


def configure_logging() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    numeric_level = getattr(logging, log_level, None)

    if not isinstance(numeric_level, int):
        raise ValueError(
            f"Invalid logging level: {log_level}"
        )

    logging.basicConfig(
        level=numeric_level,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
    )