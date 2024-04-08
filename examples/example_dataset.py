from octoflow.data.dataset import Dataset

data = [
    {"_id": 1, "name": "Alice", "age": 25},
    {"_id": 2, "name": "Bob", "age": 30},
]

dataset = Dataset(data)

docs = dataset[0:2]

print(docs)
