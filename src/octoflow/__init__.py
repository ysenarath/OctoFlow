from typing import Optional

from octoflow import logging
from octoflow.config import Config
from octoflow.plugin import Package
from octoflow.tracking import Experiment, Run, TrackingClient, Value

__version__ = "0.0.19"

__all__ = [
    "TrackingClient",
    "Experiment",
    "Run",
    "Value",
    "Config",
    "LoggingFactory",
    "logging",
]

# import default plugins if available

default_plugins_package: Optional[Package]

try:
    from octoflow_plugins import package as default_plugins_package  # type: ignore
except ImportError:
    default_plugins_package = None

if default_plugins_package is not None:
    default_plugins_package.import_modules()
