from __future__ import annotations

import os
from collections import UserDict, deque
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple, Union

__all__ = [
    "flatten",
    "ValueTree",
]


def flatten(
    data: Dict[str, Any],
    parent_key: Optional[str] = None,
    separator: str = ".",
) -> dict[str, Any]:
    """
    Flatten a nested dictionary.

    Parameters
    ----------
    data : Dict[str, Any]
        The nested dictionary to flatten.
    parent_key : Optional[str], optional
        The parent key, by default None
    separator : str, optional
        The separator, by default "."

    Returns
    -------
    dict[str, Any]
        The flattened dictionary.
    """
    if parent_key is None:
        parent_key = ""
    items = []
    for key, value in data.items():
        # escape dots
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, Mapping):
            items.extend(
                flatten(
                    value,
                    parent_key=new_key,
                    separator=separator,
                ).items()
            )
        else:
            items.append((new_key, value))
    return dict(items)


ValueTreeType = MutableMapping[str, Union[str, int, float, bool, None, "ValueTree"]]


class ValueTree(UserDict, ValueTreeType):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


def value_tree(
    root: Dict[int, Any],
    nodes: Dict[int, Tuple[str, Any]],
    /,
    path="/",
) -> ValueTree:
    """Create a value tree from a nested dictionary.

    Parameters
    ----------
    root : Dict[int, Any]
        The nested dictionary.
    nodes : Dict[int, Tuple[str, Any]]
        The nodes.
    path : str, optional
        The path, by default "/"

    Returns
    -------
    ValueTree
        The value tree.
    """
    tree = ValueTree()
    for node_id, inner in root.items():
        key, value = nodes[node_id]
        key_path = os.path.join(path, key)
        value_path = os.path.join(key_path, str(value))
        if inner is None:
            if key in tree:
                msg = f"key path '{key_path}' already exists"
                raise ValueError(msg)
            tree[key] = value
        elif key in tree:
            temp: ValueTree = tree[key]
            if not isinstance(temp, ValueTree):
                msg = f"expected 'ValueTree' for key path '{key_path}', got '{type(temp).__name__}'"
                raise ValueError(msg)
            temp.update({
                value: value_tree(inner, nodes, path=value_path),
            })
        else:
            subtree = ValueTree()
            subtree.update({
                value: value_tree(inner, nodes, path=value_path),
            })
            tree[key] = subtree
    return tree


class MergedValueTree:
    def __init__(self, /, root: MergedValueTree = None) -> None:
        self.data = {}
        self.length = 0 if root is None else root
        self.root = self if root is None else root

    def _basic_insert(self, tree: ValueTree) -> None:
        for key, value in tree.items():
            if isinstance(value, ValueTree):
                subtree = self.data.get(key, None)
                if subtree is None:
                    subtree = MergedValueTree(root=self.root)
                subtree._basic_insert(value)
                self.data[key] = subtree
            else:
                if key not in self.data:
                    self.data[key] = []
                values: List = self.data[key]
                for _ in range(self.root.length - len(values)):
                    values.append(None)
                values.append(value)
        if self.root is self:
            # only root will update min_length
            self.root.length = self.root.length + 1

    def insert(self, tree: ValueTree) -> None:
        self._basic_insert(tree)

    def extend(self, trees: List[ValueTree]) -> None:
        for tree in trees:
            self._basic_insert(tree)

    def __len__(self) -> int:
        return self.root.length

    def __repr__(self) -> str:
        return f"{self.data!r}"
