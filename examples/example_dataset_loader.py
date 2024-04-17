from typing import List, Union

from octoflow.data import load_dataset, schema
from octoflow.data.dataclass import BaseModel, field, fieldset

dset = load_dataset("jsonl", "./examples/*.jsonl")


class Address(BaseModel):
    street: str = field()
    city: str = field()
    state: str = field()
    zip: str = field()


class User(BaseModel):
    name: str = field()
    age: Union[float, int] = field()
    address: Address = field()
    phone: str = field()
    emails: List[str] = field()


s = schema.from_dataclass(User)

print(s)

print(
    fieldset(User).address({
        "address": {
            "street": "123 Main St",
            "city": "Springfield",
            "state": "IL",
            "zip": "62701",
        }
    })
)
