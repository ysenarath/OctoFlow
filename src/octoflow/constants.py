from dataclasses import _MISSING_TYPE, MISSING  # noqa: PLC2701

__all__ = [
    "DEFAULT",
    "MISSING",
    "DefaultType",
    "MissingType",
]

MissingType = _MISSING_TYPE


class DefaultType:
    pass


DEFAULT = DefaultType()
