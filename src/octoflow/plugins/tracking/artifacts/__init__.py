from octoflow.plugins.package import Package

__all__ = [
    "package",
]

package = Package(
    "artifacts",
    modules=[
        {
            "name": ".json_",
            "package": "octoflow.plugins.tracking.artifacts",
        },
        {
            "name": ".pandas_",
            "package": "octoflow.plugins.tracking.artifacts",
        },
        {
            "name": ".pickle_",
            "package": "octoflow.plugins.tracking.artifacts",
        },
        {
            "name": ".transformers_",
            "package": "octoflow.plugins.tracking.artifacts",
        },
    ],
)
