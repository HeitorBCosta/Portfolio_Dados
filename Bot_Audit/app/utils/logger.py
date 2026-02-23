import logging
from logging import Logger

def get_logger(name: str) -> Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # evita log duplicado

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger