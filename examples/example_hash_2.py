from __future__ import annotations

from sentence_transformers import SentenceTransformer

from octoflow.utils.hashutils import decorate_init, hash, use_init_based_hash

decorate_init(SentenceTransformer)

sentence_model_1 = SentenceTransformer("all-MiniLM-L6-v2")

use_init_based_hash(sentence_model_1)

print(hash(sentence_model_1))
