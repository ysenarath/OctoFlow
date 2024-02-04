from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from octoflow.utils.dataclasses import BaseModel

if TYPE_CHECKING:
    from octoflow.tracking.store import TrackingStore


class Base(BaseModel):
    def __post_init__(self) -> None:
        self._store = None

    @property
    def store(self) -> Optional[TrackingStore]:
        return self._store

    @store.setter
    def store(self, store: TrackingStore) -> None:
        self._store = store
