from pathlib import Path

from octoflow.exceptions import ValidationError
from octoflow.utils.validate import validator


class HasPath:
    @validator
    def path(self, value) -> Path:  # noqa: PLR6301
        if not isinstance(value, (Path, str)):
            msg = f"expected str or Path, got {type(value).__name__}"
            raise ValidationError(msg)
        try:
            return Path(value)
        except Exception as e:
            msg = f"invalid path: {value}"
            raise ValidationError(msg) from e


print(HasPath.path)

a = HasPath()
a.path = "."
print(type(a.path))

try:
    a.path = 1
except ValidationError as e:
    print(e)
