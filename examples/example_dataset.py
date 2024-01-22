import shutil
from pathlib import Path

import pandas as pd

from octoflow.data.dataset import Dataset
from octoflow.utils import resources

# clear all resources
resources_path: Path = resources.get_resources_path()

if resources_path.exists():
    shutil.rmtree(resources_path)


data = [
    {"a": 1, "b": 2, "c": 3},
    {"a": 4, "b": 5, "c": 6},
    {"a": 7, "b": 8, "c": 9},
]

dataset = Dataset(data)

print(dataset.path)


data = {"a": [1, 4, 7], "b": [2, 5, 8], "c": [3, 6, 9]}

df = pd.DataFrame(data)
dataset = Dataset(df)

if resources_path.exists():
    shutil.rmtree(resources_path)
