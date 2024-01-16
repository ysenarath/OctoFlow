from __future__ import annotations

import copy
from typing import Any, Dict, List, Mapping, Optional


class ValueTuple(tuple):
    """A value tuple."""

    def __new__(cls, key: str, value: Any, is_step: Optional[bool] = None) -> ValueTuple:
        return super(ValueTuple, cls).__new__(cls, (key, value))

    def __init__(self, key: str, value: Any, is_step: Optional[bool]):
        self.is_step = is_step

    @property
    def key(self) -> str:
        """Get the key."""
        return self[0]

    @property
    def value(self) -> Any:
        """Get the value."""
        return self[1]

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ValueTuple):
            if isinstance(other, tuple):
                other = ValueTuple(*other)
            else:
                return False
        return self.key == other.key and self.value == other.value

    def __hash__(self) -> int:
        return hash((self.key, self.value))

    def __repr__(self) -> str:
        return f"({self.key}, {self.value})"


class ValueTree(Mapping[int, "ValueTree"]):
    """A value tree."""

    def __init__(
        self,
        data: Mapping[int, dict],
        id2value: Dict[int, ValueTuple],
        root: bool = True,
    ):
        super(ValueTree, self).__init__()
        self.data = data
        self.id2value = id2value
        self.root = root

    def __getitem__(self, key: int) -> ValueTree:
        subtree = self.data[key]
        return ValueTree(subtree, id2value=self.id2value, root=False)

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def select(
        self,
        *keys: str,
    ) -> List[dict]:
        """
        Select values from the tree.

        Parameters
        ----------
        keys : str
            The keys to select.

        Returns
        -------
        List[dict]
            A list of dictionaries containing the selected values.
        """
        if len(keys) == 0 or len(self) == 0:
            # either the keys are empty or the tree is empty
            return []
        record = {}
        remaining_keys = set(keys)
        for value_id, subtree in self.items():
            # add all the values that are not steps if they are in the keys
            value = self.id2value[value_id]
            if value.is_step:
                continue
            if len(subtree) != 0:
                # child is a parent
                msg = f"variable '{value.key}' is not marked with 'is_step'"
                raise ValueError(msg)
            if value.key in keys:
                # parent is the key
                remaining_keys = remaining_keys - {value.key}
                record[value.key] = value.value
        if len(remaining_keys) == 0:
            # no keys left
            return [record]
        output = []
        for value_id, subtree in self.items():
            value = self.id2value[value_id]
            if not value.is_step:
                continue
            out = copy.deepcopy(record)
            has_key = value.key in remaining_keys
            if has_key:
                out[value.key] = value.value
            subtree_remaining_keys = remaining_keys - {value.key}
            if len(subtree_remaining_keys) == 0:
                # remaining key was found as step variable
                output.append(out)
                continue
            subtree_selection = subtree.select(*subtree_remaining_keys)
            if len(subtree_selection) > 0 and not has_key:
                # looks like user selected a key without their parent step value
                msg = f"missing '{value.key}'"
                raise ValueError(msg)
            for subtree_out in subtree_selection:
                # there are keys left but the subtree does not contain them
                subtree_out.update(out)
                output.append(subtree_out)
        return output

    def normalize(self) -> dict:
        """
        Normalize a value tree by removing IDs and replacing them with key-value pairs.

        Returns
        -------
        dict
            The normalized value tree.
        """
        normalized_tree = {}
        for parent_id, subtree in self.items():
            if parent_id is None:
                msg = "key in ValueTree must not be None"
                raise ValueError(msg)
            parent = self.id2value[parent_id]
            if not isinstance(subtree, ValueTree):
                msg = f"subtree must be of type ValueTree, got {type(subtree).__name__}"
                raise ValueError(msg)
            normalized_tree[parent] = subtree.normalize()
        return normalized_tree

    def __repr__(self):
        return f"{self.__class__.__name__}({self.data})"


def build_trees(values: List) -> Dict[int, ValueTree]:
    """
    Build a value tree from a list of values.

    Parameters
    ----------
    values : List
        A list of values containing tuples of (run_id, value_id, step_id, is_step, key, value).

    Returns
    -------
    Dict[int, dict]
        A dictionary representing the value tree, where the keys are run IDs and the values are nested dictionaries.
    """
    trees: dict = {}
    id2value = {}
    for run_id, value_id, step_id, is_step, key, value in values:
        id2value[value_id] = ValueTuple(key, value, is_step=is_step)
        if run_id not in trees:
            trees[run_id] = {}
        root: dict = trees[run_id]
        if step_id not in root or root[step_id] is None:
            root[step_id] = {}
        node = root[value_id] if value_id in root else root.setdefault(value_id, {})
        root[step_id][value_id] = node
    return {
        run_id: ValueTree(
            root[None],
            id2value=id2value,
        )
        for run_id, root in trees.items()
    }


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


def flatten(
    data: Dict[str, Any],
    parent_key: str = "",
    separator: str = ".",
) -> dict[str, Any]:
    """
    Flatten a nested dictionary.

    Parameters
    ----------
    data : Dict[str, Any]
        The nested dictionary to flatten.

    Returns
    -------
    dict[str, Any]
        The flattened dictionary.
    """
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
