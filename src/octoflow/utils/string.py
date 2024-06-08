import re
import textwrap
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


def shorten(x, dtype):
    """Shorten the string representation of a value.

    Parameters
    ----------
    x : Any
        The value to be shortened.
    dtype : type
        The type of the value.

    Returns
    -------
    str
        The shortened string representation of the value.
    """
    x = textwrap.shorten(str(x), width=20)
    return "'" + str(x) + "'" if dtype is str else str(x)


def repr_list(data, dtype=None):
    """Return a string representation of a list.

    Parameters
    ----------
    data : list
        The list to be represented.
    dtype : type
        The type of the list elements.

    Returns
    -------
    str
        The string representation of the list.
    """
    result = "["
    if len(data) > 0:
        result += shorten(str(data[0]), dtype)
    if len(data) > 1:
        result += ", " + shorten(str(data[1]), dtype)
    if len(data) > 3:
        result += ", ..."
    if len(data) > 2:
        result += ", " + shorten(str(data[-1]), dtype)
    result += "]"
    return result
