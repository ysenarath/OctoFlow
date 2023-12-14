from __future__ import annotations

from typing import Optional, Union

from sqlalchemy import String


class Namespace:
    def __init__(
        self,
        name: Union[str, Namespace, None] = None,
    ) -> None:
        if isinstance(name, Namespace):
            name = name.name
        elif name is None:
            name = ""
        elif name != "" and not all(item.isidentifier() for item in name.split(".")):
            msg = "variable names must contain valid python identifiers"
            raise ValueError(msg)
        items = name.rsplit(".", maxsplit=1)
        n_items = len(items)
        if n_items == 2:  # noqa: PLR2004
            self._namespace_name, self._local_name = items
        else:
            self._namespace_name, self._local_name = "", items[0]

    @property
    def local_name(self) -> str:
        return self._local_name

    @property
    def namespace_name(self) -> str:
        return self._namespace_name

    @property
    def name(self) -> str:
        if len(self._namespace_name) == 0:
            return self._local_name
        return f"{self._namespace_name}.{self._local_name}"

    def __getattr__(self, key: str) -> Namespace:
        if not key.isidentifier():
            msg = "variable names must contain valid python identifiers"
            raise ValueError(msg)
        ns = Namespace()
        if len(self._namespace_name) == 0:
            ns._namespace_name = self._local_name
            ns._local_name = key
        else:
            ns._namespace_name = f"{self._namespace_name}.{self._local_name}"
            ns._local_name = key
        return ns

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Namespace):
            other = other.name
        if isinstance(other, str):
            return self.name == other
        return False

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> Optional[str]:
        return self.name


class NamespaceComparatorFactory(String.Comparator):  # noqa: PLW1641
    def __eq__(self, other: Union[Namespace, str]):
        if isinstance(other, Namespace):
            return self.expr == str(other.name)
        elif isinstance(other, str):
            return super(NamespaceComparatorFactory, self).__eq__(other)
        return False


class NamespaceType(String):
    comparator_factory = NamespaceComparatorFactory
