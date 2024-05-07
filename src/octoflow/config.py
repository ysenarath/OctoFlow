from dataclasses import dataclass, field
from pathlib import Path

from octoflow.utils.config import Config

__all__ = [
    "config",
]


@dataclass
class CacheConfig:
    path: Path = "${oc.select:resources.path}/cache"


@dataclass
class ResourcesConfig:
    path: Path = "~/.octoflow"
    # for user data
    cache: CacheConfig = field(default_factory=CacheConfig)


@dataclass
class LoggingConfig:
    level: str = "INFO"
    format: str = "%(asctime)s %(levelname)s %(name)s [%(pathname)s:%(lineno)s] %(message)s"


@dataclass
class OctoFlowConfig:
    resources: ResourcesConfig = field(default_factory=ResourcesConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


config = Config(OctoFlowConfig)
