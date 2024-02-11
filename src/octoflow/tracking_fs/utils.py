from __future__ import annotations

from typing import Any, Dict, Mapping

__all__ = [
    "flatten",
    "validate_slug",
]


def flatten(data: Dict[str, Any], parent_key: str = "", separator: str = ".") -> dict[str, Any]:
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


def validate_slug(s: str) -> bool:
    if s.startswith("."):
        return False
    return all(c.isalnum() or (c in ".-_") for c in s)
