import importlib


class Package:
    def __init__(self, name: str, modules: list):
        """
        Package listing all modules to expose.

        Parameters
        ----------
        name : str
            Name of the package.
        modules : list
            List of modules to expose. Each module can be a `string` or a `dict`.

            If a `string`, it is the name of the module to import.

            If a `dict`, with keys:
            `name` indicating name of the module to import.
            `package` indicating the name of the package to import the module from.
            If not specified, the module is imported from the current package.
        """
        self.name = name
        self.modules = modules

    def import_modules(self):
        """
        Import all modules in the package.

        The modules are imported in the order they are defined in the package.

        Returns
        -------
        None
            The modules are imported silently. If an error occurs, it is raised.

        Raises
        ------
        TypeError
            If a module is not a `string` or a `dict`.
        ImportError
            If a module cannot be imported.
        """
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
