import os
from collections import UserDict
from typing import Any, Dict, Generic, Mapping, MutableMapping, Optional, TypeVar, Union

__all__ = [
    "flatten",
    "value_tree",
    "ValueTree",
]

T = TypeVar("T")


class ValueTreeBase(
    UserDict,
    MutableMapping[str, Union[str, int, float, bool, None, T]],
    Generic[T],
):
    pass


ValueTree = ValueTreeBase[ValueTreeBase]


def value_tree(root: Dict[str, Any], /, path="/") -> ValueTree:
    tree = ValueTree()
    for (key, value), inner in root.items():
        key_path = os.path.join(path, key)
        value_path = os.path.join(key_path, str(value))
        if inner is None:
            if key in tree:
                msg = f"key path '{key_path}' already exists"
                raise ValueError(msg)
            tree[key] = value
        elif key in tree:
            temp = tree[key]
            if not isinstance(temp, MutableMapping):
                msg = f"expected 'dict' for key path '{key_path}', got '{type(temp).__name__}'"
                raise ValueError(msg)
            temp.update({
                value: value_tree(inner, path=value_path),
            })
        else:
            tree[key] = ValueTree({
                value: value_tree(inner, path=value_path),
            })
    return tree


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
