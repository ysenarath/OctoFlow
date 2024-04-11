import shutil
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Optional, Union

from octoflow import config

__all__ = [
    "get_resources_path",
    "get_cache_path",
    "cache",
]


def get_resources_path(path: Optional[str] = None) -> Path:
    if path is None:
        # supress error
        with suppress(AttributeError):
            path = config.resources.path
    return Path(path).expanduser()


def get_cache_path(path: Union[str, Path, None] = None) -> Path:
    if path is None:
        with suppress(AttributeError):
            path = config.resources.cache.path
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
