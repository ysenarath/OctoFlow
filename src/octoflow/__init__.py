import contextlib

from octoflow import logging
from octoflow.config import Config, config
from octoflow.tracking import Experiment, Run, TrackingClient, Value

__version__ = "0.0.33"

__all__ = [
    "TrackingClient",
    "Experiment",
    "Run",
    "Value",
    "Config",
    "LoggingFactory",
    "logging",
    "logger",
]

# create the octoflow root logger
logger = logging.get_logger(
    name=next(iter(__package__.split("."))),  # octoflow logger
    level=config.logging.level,
    handlers="console",
    formatter=config.logging.format,
)

# import default plugins if available
with contextlib.suppress(ImportError):
    from octoflow_plugins import package as default_plugins_package

    default_plugins_package.import_modules()
