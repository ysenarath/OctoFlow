from dataclasses import _MISSING_TYPE, MISSING

__all__ = [
    "MISSING",
    "MissingType",
    "DefaultType",
    "DEFAULT",
]

MissingType = _MISSING_TYPE


class DefaultType:
    pass


DEFAULT = DefaultType()
