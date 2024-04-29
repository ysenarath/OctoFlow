import time
from typing import List, Union

from octoflow.data.dataclass import BaseModel, field
from octoflow.data.dataset import Dataset


def generate_test_data():
    for i in range(1000):
        yield [
            {
                "id": f"{j}-{i}",
                "name": "Alice",
                "age": 30,
                "address": [
                    {
                        "street": f"{i} Main St",
                        "city": "Springfield",
                        "state": "IL",
                        "zip": "62701",
                    }
                ],
                "phone": f"{i}{i}{i}-1233-442",
                "emails": [],
            }
            for j in range(1)
        ]


class Address(BaseModel):
    street: str
    city: str
    state: str
    zip: str


class User(BaseModel):
    name: str
    age: Union[float, int]
    address: List[Address]
    phone: str
    emails: List[str] = field(default_factory=list)


start_time = time.time()
dataset = Dataset(generate_test_data(), schema=User)
end_time = time.time()

execution_time = end_time - start_time
print("Execution time with schema:", execution_time)

start_time = time.time()
dataset = Dataset(generate_test_data())
end_time = time.time()

execution_time = end_time - start_time
print("Execution time without schema:", execution_time)
