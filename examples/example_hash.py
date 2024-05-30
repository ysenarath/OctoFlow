from __future__ import annotations

from sentence_transformers import SentenceTransformer

from octoflow.utils.hashing import hash, hashable, init_based_hash

SentenceTransformerH = init_based_hash(SentenceTransformer)


def not_depended():
    return "Not depended"


@hashable()
def dependent():
    print("Loading model")
    return SentenceTransformerH("all-MiniLM-L6-v2")


@hashable(depends_on=[dependent])
def main():
    model = dependent()
    return model


print(hash(main))
