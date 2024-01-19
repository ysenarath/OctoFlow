import pickle  # noqa: S403
from typing import Any

from octoflow.tracking.artifact.handler import ArtifactHandler

__all__ = [
    "PickleArtifactHandler",
]


class PickleArtifactHandler(ArtifactHandler, name="pickle"):
    @classmethod
    def can_handle(cls, obj: Any) -> bool:
        return False

    def exists(self) -> bool:
        return (self.path / "data.pickle").exists()

    def load(self) -> Any:
        protocol = self.metadata.get("protocol", 4)
        with open(self.path / "data.pickle", "rb") as f:
            return pickle.load(f, protocol=protocol)  # noqa: S301

    def save(self, obj, protocol: int = 4):
        self.metadata["protocol"] = protocol
        with open(self.path / "data.pickle", "wb") as f:
            pickle.dump(obj, f, protocol=protocol)
