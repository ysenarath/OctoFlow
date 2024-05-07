import contextlib

from octoflow import logging
from octoflow.config import config
from octoflow.tracking import Experiment, Run, TrackingClient, Value
from octoflow.utils.config import Config

__version__ = "0.0.55"

__all__ = [
    "Config",
    "Experiment",
    "LoggingFactory",
    "Run",
    "TrackingClient",
    "Value",
    "config",
    "logger",
    "logging",
    "server",
]

logging.logger = logging.get_logger(
    name="octoflow",
    level=config.logging.level,
    formatter=config.logging.format,
)

default_plugins_package = None

with contextlib.suppress(ImportError):
    from octoflow_plugins import package as default_plugins_package

if default_plugins_package:
    default_plugins_package.import_modules()
