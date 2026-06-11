"""Tiny logging helper so every module logs consistently."""
import logging


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        # Under Airflow the root handler already exists; only add one when run standalone.
        logger.setLevel(logging.INFO)
    return logger
