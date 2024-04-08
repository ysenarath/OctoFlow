import re

ESCAPED_CHARS = r"\\`*_{}[]()#+\-.!"
ESCAPED_CHARS_RE = re.compile(r"([%s])" % re.escape(ESCAPED_CHARS))
UNESCAPED_CHARS_RE = re.compile(r"\\([%s])" % re.escape(ESCAPED_CHARS))


def escape(text: str) -> str:
    """Escape text."""
    return ESCAPED_CHARS_RE.sub(r"\\\1", text)


def unescape(text: str) -> str:
    """Unescape text."""
    return UNESCAPED_CHARS_RE.sub(r"\1", text)
