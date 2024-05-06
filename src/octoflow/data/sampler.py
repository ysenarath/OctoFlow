from typing import Mapping, Sequence

import numpy as np
import pandas as pd

__all__ = [
    "Sampler",
]


class Sampler:
    def __init__(self, columns: Mapping[str, int]):
        self.args = (0, *list(columns.values()))
        self.columns = list(columns.keys())

    def __call__(self, lst: Sequence[int]):
        lst = np.array(lst)
        np.random.shuffle(lst)
        return pd.Series(
            [
                lst[self.args[i] : self.args[i + 1]]
                for i in range(len(self.args) - 1)
            ],
            index=self.columns,
        )
