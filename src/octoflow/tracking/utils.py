from __future__ import annotations

import copy
import os
from collections import UserDict
from typing import Any, Dict, Mapping, MutableMapping, Optional, Tuple, Union

__all__ = [
    "flatten",
    "TreeNode",
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


class TreeNode(UserDict, MutableMapping[str, Union[str, int, float, bool, None, "TreeNode"]]): ...


class ValueNode(UserDict, MutableMapping[Union[str, int, float, bool, None], TreeNode]): ...


def value_tree(
    root: Dict[int, Any],
    nodes: Dict[int, Tuple[str, Any]],
    /,
    path="/",
) -> TreeNode:
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
    tree = TreeNode()
    for node_id, inner in root.items():
        key, value = nodes[node_id]
        key_path = os.path.join(path, str(key))
        value_path = os.path.join(key_path, str(value))
        if inner is None:
            if key in tree:
                msg = f"key path '{key_path}' already exists"
                raise ValueError(msg)
            tree[key] = value
        elif key in tree:
            temp = tree[key]
            if not isinstance(temp, ValueNode):
                msg = f"expected 'ValueNode' for key path '{key_path}', got '{type(temp).__name__}'"
                raise ValueError(msg)
            temp.update({
                value: value_tree(inner, nodes, path=value_path),
            })
        else:
            subtree = ValueNode()
            subtree.update({
                value: value_tree(inner, nodes, path=value_path),
            })
            tree[key] = subtree
    return tree


def index_tree(
    tree: TreeNode,
    /,
    base_dict: Optional[dict] = None,
    ancestor_keys: Optional[Tuple[str]] = None,
) -> dict:
    if base_dict is None:
        base_dict = {}
    if ancestor_keys is None:
        ancestor_keys = ()
    for key, value in tree.items():
        if isinstance(value, ValueNode):
            continue
        pkey = (*ancestor_keys, key)
        base_dict[pkey] = value
    branches = {}
    index = tuple(sorted(base_dict.keys()))
    branches[index] = base_dict
    for key, values in tree.items():
        if not isinstance(values, ValueNode):
            continue
        pkey = (*ancestor_keys, key)
        for value, nested in values.items():
            base_dict_copy = copy.deepcopy(base_dict)
            base_dict_copy[pkey] = value
            if not isinstance(nested, TreeNode):
                msg = f"expected 'TreeNode', got '{type(nested).__name__}'"
                raise ValueError(msg)
            branches.update(
                index_tree(
                    nested,
                    base_dict=base_dict_copy,
                    ancestor_keys=pkey,
                )
            )
    return branches
