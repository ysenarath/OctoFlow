import time
from typing import List, Union

import pyarrow as pa

from octoflow import logging
from octoflow.data.dataclass import BaseModel, field
from octoflow.data.dataset import Dataset

logging.set_level(logging.DEBUG)

logger = logging.get_logger(__name__, logging.DEBUG)


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


def make_upper(table: pa.Table):
    text_input = table.column("name").to_pylist()
    return {"upper_name": list(map(str.upper, text_input))}


dataset = dataset.map(make_upper, batched=True)

dataset = dataset.rename({"name": "original_name"})

logger.info(dataset[0])
