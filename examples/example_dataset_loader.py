from octoflow.data import load_dataset

dset = load_dataset("jsonl", "./examples/*.jsonl")

print("Dataset loaded:")
print(f"  - Number of samples: {len(dset)}")
print(f"  - Sample keys: {dset[0].keys()}")
