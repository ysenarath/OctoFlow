from typing import Tuple

__all__ = [
    "join",
    "parse",
]


def join(*names) -> str:
    return ".".join([n for n in names if n is not None and len(n) > 0])


def parse(name: str) -> Tuple[str, str]:
    parts = name.rsplit(".", maxsplit=1)
    if len(parts) == 2:  # noqa: PLR2004
        return parts
    return "", parts[-1]
