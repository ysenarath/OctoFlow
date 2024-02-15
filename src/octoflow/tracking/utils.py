from __future__ import annotations

import os
from collections import UserDict
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple, Union

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


def merge_tree(
    tree: ValueTree,
    into: Optional[ValueTree] = None,
    /,
    length: int = 0,
) -> Tuple[ValueTree, int]:
    """Merge a value tree into another value tree.

    Parameters
    ----------
    tree : ValueTree
        The value tree to merge.
    into : Optional[ValueTree], optional
        The value tree to merge into, by default None

    Returns
    -------
    Tuple[ValueTree, int]
        The merged value tree and the new length.
    """
    if into is None:
        into = ValueTree()
    for key, value in tree.items():
        if isinstance(value, ValueTree):
            into[key] = merge_tree(value, into.get(key, None), length=length)
        elif key in into:
            into[key] += [value]
        else:
            into[key] = [None for _ in range(length)] + [value]
    return into, length + 1
