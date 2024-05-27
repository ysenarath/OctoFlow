import contextlib

from octoflow import logging
from octoflow.config import config
from octoflow.utils.config import Config

__version__ = "0.1.0"

__all__ = [
    "Config",
    "config",
    "logger",
    "logging",
]

logging.logger = logging.get_logger(
    name="octoflow",
    level=config.logging.level,
    formatter=config.logging.format,
)

with contextlib.suppress(ImportError):
    from octoflow_plugins import package as default_plugins_package

if "default_plugins_package" in locals():
    default_plugins_package.import_modules()
