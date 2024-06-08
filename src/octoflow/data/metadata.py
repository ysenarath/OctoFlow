import json
from pathlib import Path
from typing import Any, Dict, Generator, Optional, Union

from octoflow.utils.collections import MutableDict

__all__ = [
    "MetadataFile",
    "unify_metadata",
]


class Metadata(MutableDict[str, Any]): ...


class MetadataFile(Metadata):
    def __init__(self, __path: Union[str, Path]) -> None:
        path = Path(__path)
        if path.is_dir():
            msg = f"metadata path '{path}' is a directory"
            raise ValueError(msg)
        self._path = path
        data = self._load_data()
        super().__init__(data)
        self.add_event_listener("change", self._on_change)

    @property
    def path(self) -> Path:
        return self._path

    def _on_change(self) -> Generator[Dict[str, Any], None, None]:
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._data, f)

    def _load_data(self) -> Dict[str, Any]:
        if not self._path.exists():
            return {}
        with open(self._path, encoding="utf-8") as f:
            return json.load(f)


def get_metadata(obj: Any) -> Metadata:
    metadata = None
    if hasattr(obj, "metadata"):
        metadata = obj.metadata
    # convert to Metadata
    if metadata is None:
        return Metadata()
    if isinstance(metadata, Metadata):
        return metadata
    return Metadata(metadata)


def unify_metadata(left: Any, right: Any) -> Optional[dict]:
    # merge metadata left then right
    #   (right metadata will overwrite left metadata)
    metadata_l = get_metadata(left)
    metadata_r = get_metadata(right)
    if metadata_l is None:
        return metadata_r
    if metadata_r is None:
        return metadata_l
    return {**metadata_l, **metadata_r}
