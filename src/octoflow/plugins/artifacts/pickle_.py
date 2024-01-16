import pickle
from pathlib import Path
from typing import Any, Union

from octoflow.core.artifact.handler import ArtifactHandler

__all__ = [
    "PickleArtifactHandler",
]


class PickleArtifactHandler(ArtifactHandler, name="pickle"):
    def __init__(self, path: Union[Path, str], protocol: int = 4):
        super().__init__(path)
        self.protocol = protocol

    @classmethod
    def can_handle(cls, obj: Any) -> bool:
        return False

    def load(self) -> Any:
        with open(self.path / "data.pickle", "rb") as f:
            return pickle.load(f)  # noqa: S301

    def save(self, obj):
        with open(self.path / "data.pickle", "wb") as f:
            pickle.dump(obj, f, protocol=self.protocol)
