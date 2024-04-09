from dataclasses import dataclass, field

from octoflow.utils.config import Config

__all__ = [
    "config",
]


@dataclass
class CacheConfig:
    path: str = "{root.resources.path}/cache"


@dataclass
class ResourcesConfig:
    path: str = "~/.octoflow"
    cache: CacheConfig = field(default_factory=CacheConfig)


@dataclass
class LoggingConfig:
    level: str = "INFO"
    format: str = "%(asctime)s %(levelname)s %(name)s %(message)s"


@dataclass
class OctoFlowConfig:
    resources: ResourcesConfig = field(default_factory=ResourcesConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


config = Config.structured(OctoFlowConfig)
