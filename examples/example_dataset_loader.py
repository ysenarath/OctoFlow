from dataclasses import field
from typing import List, Union

from octoflow.data import load_dataset, schema
from octoflow.data.dataclass import BaseModel, fields
from octoflow.data.dataset import Dataset

dset = load_dataset("jsonl", "./examples/*.jsonl")


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


s = schema.from_dataclass(User)

print(s)

fs = fields(User)

data = {
    "address": [
        {
            "street": "123 Main St",
            "city": "Springfield",
            "state": "IL",
            "zip": "62701",
        }
    ]
}

print(fs.address(data))

print(fs.address > 0)
