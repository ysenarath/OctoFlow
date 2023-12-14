import logging
from typing import Optional, Sequence, Union

CONSOLE_HANDLER = "console"

PACKAGE_NAME = next(iter(__name__.split(".")))
DEFAULT_LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LOGGING_HANDLERS = {
    CONSOLE_HANDLER,
}


class LoggingFactory:
    def __init__(
        self,
        name: Optional[str] = None,
        level: Union[int, str] = logging.DEBUG,
        formatter: str = DEFAULT_LOGGING_FORMAT,
        handlers: Sequence[str] = DEFAULT_LOGGING_HANDLERS,
    ) -> None:
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if CONSOLE_HANDLER in handlers:
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


get_logger = LoggingFactory(PACKAGE_NAME)
