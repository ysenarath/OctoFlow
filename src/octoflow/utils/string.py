import re
from typing import Generator

__all__ = [
    "escape",
    "unescape",
]

ESCAPED_CHARS = r"\\`*_{}[]()#+\-.!"
ESCAPED_CHARS_RE = re.compile(rf"([{re.escape(ESCAPED_CHARS)}])")
UNESCAPED_CHARS_RE = re.compile(rf"\\([{re.escape(ESCAPED_CHARS)}])")


def escape(text: str) -> str:
    """Escape text."""
    return ESCAPED_CHARS_RE.sub(r"\\\1", text)


def unescape(text: str) -> str:
    """Unescape text."""
    return UNESCAPED_CHARS_RE.sub(r"\1", text)


def _split_iter(string: str, delimiter: str) -> Generator[str, None, None]:
    ln = len(string)
    i = 0
    j = 0
    while j < ln:
        if string[j] == "\\":
            if j + 1 >= ln:
                yield string[i:j]
                return
            j += 1
        elif string[j] == delimiter:
            yield unescape(string[i:j])
            i = j + 1
        j += 1
    if i != j:
        yield unescape(string[i:j])


def split(text: str, delimiter: str) -> list:
    """Split text by delimiter."""
    return list(_split_iter(text, delimiter))


def join(parts: list, delimiter: str) -> str:
    """Join parts with delimiter."""
    return delimiter.join(map(escape, parts))
