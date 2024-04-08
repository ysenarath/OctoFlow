from typing import Optional, runtime_checkable

from typing_extensions import Protocol

__all__ = [
    "MetadataMixin",
    "unify_metadata",
]


@runtime_checkable
class MetadataMixin(Protocol):
    metadata: Optional[dict]


def unify_metadata(
    left: MetadataMixin,
    right: MetadataMixin,
) -> Optional[dict]:
    # merge metadata left then right
    #   (right metadata will overwrite left metadata)
    metadata = left.metadata
    if metadata is None:
        metadata = right.metadata
    elif right.metadata is not None:
        # metadata and right.metadata are not None
        metadata = {**metadata, **right.metadata}
    return metadata
