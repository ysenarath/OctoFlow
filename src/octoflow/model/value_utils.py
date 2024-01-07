from collections import UserDict
from collections.abc import Mapping
from typing import Any, Dict, List


def flatten(
    data: Dict[str, Any],
    parent_key: str = "",
    separator: str = ".",
) -> dict[str, Any]:
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


def build_trees_base(values: List) -> Dict[int, dict]:
    """
    Build a value tree from a list of values.

    Parameters
    ----------
    values : List
        A list of values containing tuples of (run_id, value_id, step_id, key, value).

    Returns
    -------
    Dict[int, dict]
        A dictionary representing the value tree, where the keys are run IDs and the values are nested dictionaries.
    """
    trees: dict = {}
    id2value = {}
    for run_id, value_id, step_id, key, value in values:
        id2value[value_id] = (key, value)
        if run_id not in trees:
            trees[run_id] = {}
        root: dict = trees[run_id]
        if step_id not in root or root[step_id] is None:
            root[step_id] = {}
        node = root[value_id] if value_id in root else root.setdefault(value_id, {})
        root[step_id][value_id] = node
    trees = UserDict(trees)
    trees.id2value = id2value
    return trees


def build_trees(values: List) -> Dict[int, dict]:
    """
    Build a value tree from a list of values.

    Parameters
    ----------
    values : List
        A list of values containing tuples of (run_id, value_id, step_id, key, value).

    Returns
    -------
    Dict[int, dict]
        A dictionary representing the value tree, where the keys are run IDs and the values are nested dictionaries.
    """
    trees = build_trees_base(values)
    # flatten the tree
    run_tree_map = {}
    for run_id, tree in trees.items():
        run_tree_map[run_id] = _normalize(tree, trees.id2value)
    return run_tree_map


def _normalize(tree: dict, id2value: dict) -> dict:
    """
    Normalize a value tree by removing IDs and replacing them with key-value pairs.

    Parameters
    ----------
    tree : dict
        The value tree to be normalized.
    id2value : dict
        A dictionary mapping value IDs to their corresponding key-value pairs.

    Returns
    -------
    dict
        The normalized value tree.
    """
    nominal_tree = {}
    for parent_id, child_id in tree.items():
        parent_value = id2value.get(parent_id)
        if isinstance(child_id, dict):
            nominal_tree[parent_value] = _normalize(child_id, id2value)
        else:
            child_value = id2value[child_id]
            nominal_tree[parent_value] = child_value
    return nominal_tree


def equals(
    p: dict,
    q: dict,
    /,
    partial: bool = True,
) -> bool:
    """
    Check whether two dictionaries are equal.

    Parameters
    ----------
    p : dict
        The first dictionary to compare.
    q : dict
        The second dictionary to compare.
    partial : bool, optional
        If True, performs a partial comparison where missing keys in one dictionary are ignored (default is True).

    Returns
    -------
    bool
        True if the dictionaries are equal, False otherwise.

    Raises
    ------
    ValueError
        If either `p` or `q` is not a dictionary.
    """
    if not partial:
        if p.keys() != q.keys():
            return False
        return all(equals(value, q[key]) for key, value in p.items())
    if not isinstance(p, dict) or not isinstance(q, dict):
        msg = f"cannot compare {type(p).__name__} and {type(q).__name__} types"
        raise ValueError(msg)
    for key, val_a in p.items():
        if key not in q:
            return False
        val_b = q[key]
        if not equals(val_a, val_b):
            return False
    return True
