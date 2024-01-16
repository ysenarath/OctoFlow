from __future__ import annotations

import functools
import weakref
from collections import defaultdict
from typing import Dict, Generator, Iterable, MutableMapping, MutableSequence, Optional, Set, TypeVar
from typing import MutableSet as MutableSetType

K, V = TypeVar("K"), TypeVar("V")


class EventTarget:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._event_listeners: Dict[str, Set[callable]] = defaultdict(set)

    def add_event_listener(self, type: str, listener: callable):
        self._event_listeners[type].add(listener)

    def remove_event_listener(self, type: str, listener: callable):
        self._event_listeners[type].remove(listener)

    def dispatch_event(self, event: str):
        for callback in self._event_listeners[event]:
            callback()


class MutableCollection(EventTarget):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._parent_ref: MutableCollection = None

    @property
    def _parent(self) -> Optional[MutableCollection]:
        if self._parent_ref is None:
            return None
        return self._parent_ref()

    def set_parent(self, parent: MutableCollection) -> MutableCollection:
        self._parent_ref = weakref.ref(parent)
        return self

    def changed(self):
        if self._parent is not None:
            self._parent.changed()
        self.dispatch_event("change")

    def coerce(self, value: any):
        if isinstance(value, (list, MutableList)):
            return MutableList(value).set_parent(self)
        elif isinstance(value, (dict, MutableDict)):
            return MutableDict(value).set_parent(self)
        elif isinstance(value, (set, MutableSet)):
            return MutableSet(value).set_parent(self)
        return value


class MutableDict(MutableCollection, MutableMapping[K, V]):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self._data = {}
        self.update(dict(*args, **kwargs))

    def __getitem__(self, key: K) -> V:
        return self._data[key]

    def __setitem__(self, key: K, value: V):
        self._data[key] = self.coerce(value)
        self.changed()

    def __delitem__(self, key: K):
        del self._data[key]
        self.changed()

    def __iter__(self) -> Generator[K, None, None]:
        yield from self._data

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return repr(self._data)


class MutableList(MutableCollection, MutableSequence[V]):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self._data = []
        self.extend(list(*args, **kwargs))

    def __getitem__(self, key: int) -> V:
        return self._data[key]

    def __setitem__(self, key: int, value: V):
        self._data[key] = self.coerce(value)
        self.changed()

    def insert(self, index: int, value: V):
        self._data.insert(index, self.coerce(value))
        self.changed()

    def __delitem__(self, key: int):
        del self._data[key]
        self.changed()

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return repr(self._data)

    @functools.wraps(list.sort)
    def sort(self, *args, **kwargs) -> None:
        self._data.sort(*args, **kwargs)
        self.changed()


class MutableSet(MutableCollection, MutableSetType[V]):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self._data = set()
        self.update(set(*args, **kwargs))

    def add(self, item: V):
        self._data.add(self.coerce(item))
        self.changed()

    def update(self, *s: Iterable[V]) -> None:
        self._data.update({self.coerce(i) for i in set(*s)})
        self.changed()

    def discard(self, item: V):
        self._data.discard(item)
        self.changed()

    def __contains__(self, item: V) -> bool:
        return item in self._data

    def __iter__(self) -> Generator[V, None, None]:
        yield from self._data

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return repr(self._data)
