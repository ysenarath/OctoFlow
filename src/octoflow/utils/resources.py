import tempfile
from pathlib import Path
from typing import Optional

from octoflow.config import config

__all__ = [
    "get_resources_path",
    "get_cache_path",
]


@config.wraps(name="resources")
def get_resources_path(path: str) -> Path:
    return Path(path).expanduser()


@config.wraps(name="resources.cache")
def get_cache_path(path: Optional[str]) -> Path:
    if path is not None:
        return Path(path).expanduser()
    try:
        resources_path = get_resources_path()
    except TypeError:
        resources_path = Path(tempfile.gettempdir()) / "octoflow"
    return resources_path / "cache"
