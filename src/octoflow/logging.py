import logging
from typing import Optional, Sequence, Union

__all__ = [
    "get_logger",
    "LoggingFactory",
]


class LoggingFactory:
    def __init__(
        self,
        name: Optional[str] = None,
        level: Union[int, str] = logging.INFO,
        formatter: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers: Sequence[str] = "console",
    ) -> None:
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
        self.base_logger = logger

    def __call__(self, name: Optional[str] = None) -> logging.Logger:
        return logging.getLogger(name)


get_logger = LoggingFactory(name=next(iter(__name__.split("."))))
