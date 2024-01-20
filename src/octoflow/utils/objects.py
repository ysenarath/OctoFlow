import functools
import importlib

__all__ = [
    "create_object",
]


def import_class(cls: str):
    if isinstance(cls, str):
        module, class_name = cls.rsplit(".", 1)
        module = importlib.import_module(name=module)
        cls = getattr(module, class_name)
    return cls


def create_object(
    __cls: str,  # like {module}.{class_name} or {class_name}
    __partial: bool = False,
    *args,
    **kwargs,
):
    cls = import_class(__cls)
    try:
        if __partial:
            return functools.partial(cls, *args, **kwargs)
        return cls(cls, *args, **kwargs)
    except Exception as ex:
        raise ex
