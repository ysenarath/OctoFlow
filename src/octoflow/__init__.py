from octoflow import logging, plugins
from octoflow.config import Config
from octoflow.tracking import Client, Experiment, Run, Value

__version__ = "0.0.10"

__all__ = [
    "Client",
    "Experiment",
    "Run",
    "Value",
    "Config",
    "LoggingFactory",
    "logging",
]

plugins.package.import_modules()

config = Config({
    "resources": {
        "path": "~/.octoflow",
    },
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
    },
})


logger = logging.get_logger(
    name=next(iter(__package__.split("."))),
    level=config.logging.level,
    formatter=config.logging.format,
)
