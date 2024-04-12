from octoflow.data.dataset import Dataset

data = [
    {"_id": 1, "name": "Alice", "age": 25},
    {"_id": 2, "name": "Bob", "age": 30},
    {"_id": 3, "name": "Charlie", "age": 35},
    {"_id": 4, "name": "David", "age": 40},
    {"_id": 5, "name": "Eve", "age": 45},
    {"_id": 6, "name": "Frank", "age": 50},
]

dataset = Dataset(data).map(
    lambda doc: {
        "id": doc["_id"],
        "name": doc["name"],
        "age": doc["age"],
    },
    batched=True,
    batch_size=2,
)

docs = dataset.select("name")[0:2]

print(docs)
