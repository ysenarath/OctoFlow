import shutil
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Union

from octoflow import config

__all__ = [
    "cache",
]


class Cache:
    def __init__(self, path: Union[str, Path, None] = None) -> None:
        if isinstance(path, str):
            path = Path(path).expanduser()
        if path is None:
            with suppress(AttributeError):
                path = config.resources.cache.path
        if path is None:
            with suppress(AttributeError):
                path = Path(config.resources.path).expanduser() / "cache"
        if path is None:
            path = Path(tempfile.gettempdir()) / "octoflow" / "cache"
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def clean(self):
        if self.path.exists():
            shutil.rmtree(self.path)


cache = Cache()
