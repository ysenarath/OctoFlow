import math
from typing import Any, Dict, Sequence

import numpy as np
import numpy.typing as nt
import pandas as pd

__all__ = [
    "RandomSampler",
    "Sampler",
]


class Sampler:
    def __call__(self, data: Any) -> Dict[str, Any]:
        out = self.sample(data)
        if isinstance(data, (pd.DataFrame, pd.Series)):
            return {key: data.iloc[val] for key, val in out.items()}
        return {key: data[val] for key, val in out.items()}

    def sample(self, data: Sequence[Any]) -> Dict[str, nt.NDArray[np.int64]]:
        raise NotImplementedError


class RandomSampler(Sampler):
    def __init__(self, *args, **kwargs):
        self.counts = dict(*args, **kwargs)

    def sample(self, data: Sequence[Any]) -> Dict[str, nt.NDArray[np.int64]]:
        n = len(data)
        indexes = np.arange(n)
        np.random.shuffle(indexes)
        b = 0
        args = [b]
        keys = []
        for key, size in self.counts.items():
            if isinstance(size, float) and (0 < size <= 1):
                size = n * size
            b += math.floor(size)
            args.append(b)
            keys.append(key)
        return {
            key: indexes[args[i] : args[i + 1]] for i, key in enumerate(keys)
        }
