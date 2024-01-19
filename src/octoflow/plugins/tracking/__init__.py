from octoflow.plugins.package import Package

__all__ = [
    "package",
]

package = Package(
    "tracking",
    modules=[
        {
            "name": ".artifacts",
            "package": "octoflow.plugins.tracking",
        },
    ],
)
