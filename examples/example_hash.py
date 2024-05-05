from __future__ import annotations

from sentence_transformers import SentenceTransformer

from octoflow.utils.hashing import hash, init_based_hash

SentenceTransformerH = init_based_hash(SentenceTransformer)

sentence_model = SentenceTransformerH("all-MiniLM-L6-v2")

print(hash(sentence_model))


def some_callable():
    return "hello world"


print(hash(some_callable))
