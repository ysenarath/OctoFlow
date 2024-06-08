from typing import Optional

import numpy as np

from octoflow.data import schema
from octoflow.data.dataclass import BaseModel


class Text(BaseModel):
    text: str
    embedding: Optional[np.ndarray] = None


example = Text(text="Hello, World!", embedding=np.array([1, 2, 3]))

print(schema.from_dataclass(Text))
