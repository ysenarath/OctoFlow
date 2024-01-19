import json
from collections.abc import Mapping
from typing import Any, Union

from octoflow.tracking.artifact.handler import ArtifactHandler

__all__ = [
    "JSONArtifactHandler",
]


class JSONArtifactHandler(ArtifactHandler, name="json"):
    @classmethod
    def can_handle(cls, obj: Any) -> bool:
        return isinstance(obj, Mapping)

    def exists(self) -> bool:
        return (self.path / "data.json").exists()

    def load(self):
        encoding = self.metadata.get("encoding", "utf-8")
        with open(self.path / "data.json", encoding=encoding) as f:
            return json.load(f)

    def save(
        self,
        obj,
        encoding: str = "utf-8",
        indent: Union[int, str, None] = None,
    ) -> None:
        self.metadata["encoding"] = encoding
        self.metadata["indent"] = indent
        with open(self.path / "data.json", "w", encoding=encoding) as f:
            json.dump(obj, f, indent=indent)
