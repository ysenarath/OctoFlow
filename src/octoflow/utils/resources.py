import shutil
import tempfile
from pathlib import Path
from typing import Union

from octoflow.config import config

__all__ = [
    "get_resources_path",
    "get_cache_path",
    "cache",
]


@config.wraps(name="resources")
def get_resources_path(path: str) -> Path:
    return Path(path).expanduser()


@config.wraps(name="resources.cache")
def get_cache_path(path: Union[str, Path, None]) -> Path:
    if path is not None:
        return Path(path).expanduser()
    try:
        resources_path = get_resources_path()
    except TypeError:
        resources_path = Path(tempfile.gettempdir()) / "octoflow"
    return resources_path / "cache"


class Cache:
    def __init__(self, path: Union[str, Path, None] = None) -> None:
        self._path = get_cache_path(path)

    @property
    def path(self) -> Path:
        return self._path

    def clean(self):
        if self.path.exists():
            shutil.rmtree(self.path)


cache = Cache()
