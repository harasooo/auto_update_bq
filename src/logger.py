import logging.config
from logging import Formatter, StreamHandler


def create_logger():
    logger = logging.getLogger("auto_update_bq")
    logger.setLevel(logging.INFO)
    file_handler = StreamHandler()
    handler_format = Formatter(
        "%(asctime)s:%(name)s:%(levelname)s:%(message)s")
    file_handler.setFormatter(handler_format)
    logger.addHandler(file_handler)
    return logger


if __name__ != "main":
    scrape_logger = create_logger()
