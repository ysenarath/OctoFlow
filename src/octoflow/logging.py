import logging
from typing import Optional, Union

try:
    from rich.logging import RichHandler
except ImportError:
    RichHandler = None

__all__ = [
    "get_logger",
    "set_level",
]

logger: logging.Logger

CRITICAL = logging.CRITICAL
FATAL = CRITICAL
ERROR = logging.ERROR
WARNING = logging.WARNING
WARN = WARNING
INFO = logging.INFO
DEBUG = logging.DEBUG
NOTSET = logging.NOTSET


def get_logger(
    name: Optional[str] = None,
    level: Union[int, str, None] = None,
    formatter: Optional[str] = None,
) -> logging.Logger:
    """
    Get a logger with the given name and level.

    Parameters
    ----------
    name : str, optional
        Name of the logger.
    level : int or str, optional
        Logging level.
    formatter : str, optional
        Formatter string.
    handlers : Sequence[str], optional
        Sequence of handlers to use.

    Returns
    -------
    logging.Logger
        Logger instance.
    """
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)
    if RichHandler is None:
        handler = logging.StreamHandler()
    else:
        handler = RichHandler(
            show_time=False,
            show_level=False,
            show_path=False,
        )
    handler.setLevel(logging.DEBUG)
    if not isinstance(formatter, logging.Formatter):
        # Create a formatter
        formatter = logging.Formatter(formatter)
    # add it to the handlers
    handler.setFormatter(formatter)
    # Add the handlers to the logger
    logger.addHandler(handler)
    return logger


def set_level(level: Union[int, str], logger: Optional[logging.Logger] = None):
    """
    Set the logging level of the logger.

    Parameters
    ----------
    logger : logging.Logger
        Logger instance.
    level : int or str
        Logging level.
    """
    if logger is None:
        logger = globals().get("logger", logging.getLogger())
    if isinstance(level, str):
        level = logging.getLevelName(level)
    logger.setLevel(level)
