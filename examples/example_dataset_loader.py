import pyarrow as pa

from octoflow.data import load_dataset

dataset = load_dataset("jsonl", "./examples/*.jsonl")

print("Dataset loaded:")
print(f"  - Number of samples: {len(dataset)}")
print(f"  - Sample keys: {dataset[0].keys()}")


def make_upper(table: pa.Table):
    text_input = table.column("name").to_pylist()
    return {"upper_name": list(map(str.upper, text_input))}


dataset = dataset.map(make_upper, batched=True)

dataset = dataset.rename({"name": "original_name"})

print(dataset[0])

print(dataset.path)
