__all__ = [
    "IntegrityError",
    "ValidationError",
]


class ValidationError(Exception):
    pass


class IntegrityError(Exception):
    pass
