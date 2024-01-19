import importlib


class Package:
    def __init__(self, name: str, modules: list):
        self.name = name
        self.modules = modules

    def import_modules(self):
        for module in self.modules:
            if isinstance(module, str):
                module = {"name": module}
            if not isinstance(module, dict):
                msg = f"expected str or dict, got {type(module).__name__}"
                raise TypeError(msg)
            name, package = module["name"], module.get("package")
            if package is None:
                package = __package__
            try:
                module = importlib.import_module(
                    name=name,
                    package=package,
                )
            except Exception:
                msg = f"failed to import '{name}' from '{package}' in '{self.name}'"
                raise ImportError(msg) from None
            if not hasattr(module, "__all__"):
                continue
            for name in module.__all__:
                subpackage = getattr(module, name)
                if not isinstance(subpackage, Package):
                    continue
                subpackage.import_modules()
