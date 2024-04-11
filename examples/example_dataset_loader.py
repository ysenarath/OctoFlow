import pandas as pd

from octoflow.data import load_dataset
from octoflow.data.expression import Expression

dset = load_dataset("jsonl", "./examples/*.jsonl")

n = Expression.field("name")


def map_fn(x: dict):
    x["lo_name"] = x["name"].lower()
    return x


def map_batched_fn(x: pd.DataFrame):
    x["up_name"] = x["name"].str.upper()
    return x


up_dset = (
    dset.map(map_fn)
    .map(map_batched_fn, batched=True)
    .filter(n == "Benjamin White")
)

print(up_dset[0])
