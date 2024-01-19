from octoflow.plugins.package import Package

__all__ = [
    "package",
]

package = Package(
    "plugins",
    modules=[
        {
            "name": ".tracking",
            "package": "octoflow.plugins",
        },
    ],
)
