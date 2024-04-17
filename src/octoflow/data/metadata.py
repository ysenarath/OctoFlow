from typing import Any, Optional

__all__ = [
    "unify_metadata",
]


def unify_metadata(left: Any, right: Any) -> Optional[dict]:
    # merge metadata left then right
    #   (right metadata will overwrite left metadata)
    metadata = left.metadata
    if metadata is None:
        metadata = right.metadata
    elif right.metadata is not None:
        # metadata and right.metadata are not None
        metadata = {**metadata, **right.metadata}
    return metadata
