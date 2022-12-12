import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

LOGGING_FORMAT = "[%(name)s] - %(asctime)s - %(levelname)s - %(message)s"
LOGGING_LEVEL = logging.INFO
LOGGING_DIR = os.path.join(os.path.expanduser("~"), ".alch", "logs")


def get_console_handler() -> logging.Handler:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
    return console_handler


def get_file_handler(name: str) -> logging.Handler:
    os.makedirs(LOGGING_DIR, exist_ok=True)
    filepath = os.path.join(LOGGING_DIR, "{}.log".format(name))
    file_handler = TimedRotatingFileHandler(filepath, when="midnight")
    file_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
    return file_handler


def get_module_logger(name: str, level: int = LOGGING_LEVEL) -> logging.Logger:
    """Get a logger with a default console handler and a file handler.

    Args:
        name (str): Logical name of the logger.
        level (int, optional): Logging level. Defaults to LOGGING_LEVEL.

    Returns:
        logging.Logger: Logger object.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler(name))
    logger.propagate = False
    return logger
