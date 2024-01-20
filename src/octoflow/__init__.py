from typing import Optional

from octoflow import logging
from octoflow.config import Config
from octoflow.core.plugins import Package
from octoflow.tracking import Client, Experiment, Run, Value

__version__ = "0.0.13"

__all__ = [
    "Client",
    "Experiment",
    "Run",
    "Value",
    "Config",
    "LoggingFactory",
    "logging",
]

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


default_plugins_package: Optional[Package]

try:
    from octoflow_plugins import package as default_plugins_package  # type: ignore
except ImportError:
    default_plugins_package = None

if default_plugins_package is not None:
    default_plugins_package.import_modules()
