import pyarrow as pa

from octoflow import logging
from octoflow.data import load_dataset

logging.set_level(logging.DEBUG)

logger = logging.get_logger(__name__, logging.DEBUG)

dataset = load_dataset("jsonl", "./examples/*.jsonl")

logger.info("Dataset loaded:")
logger.info(f"  - Number of samples: {len(dataset)}")
logger.info(f"  - Sample keys: {dataset[0].keys()}")


def make_upper(table: pa.Table):
    text_input = table.column("name").to_pylist()
    return {"upper_name": list(map(str.upper, text_input))}


dataset = dataset.map(make_upper, batched=True)

dataset = dataset.rename({"name": "original_name"})

logger.info(dataset[0])

logger.info(dataset.path)
