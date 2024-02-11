from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from octoflow.utils.model import ModelBase

if TYPE_CHECKING:
    from octoflow.tracking_fs.store import TrackingStore


class StoredModel(ModelBase):
    def __post_init__(self) -> None:
        self._store = None

    @property
    def store(self) -> Optional[TrackingStore]:
        return self._store

    @store.setter
    def store(self, store: TrackingStore) -> None:
        self._store = store
