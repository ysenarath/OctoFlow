import logging
from typing import Optional, Sequence, Union

__all__ = [
    "get_logger",
]


def get_logger(
    name: Optional[str] = None,
    level: Union[int, str] = logging.INFO,
    formatter: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers: Sequence[str] = "console",
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if isinstance(handlers, str):
        handlers = (handlers,)
    if "console" in handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        if not isinstance(formatter, logging.Formatter):
            # Create a formatter
            formatter = logging.Formatter(formatter)
        # add it to the handlers
        stream_handler.setFormatter(formatter)
        # Add the handlers to the logger
        logger.addHandler(stream_handler)
    return logger
