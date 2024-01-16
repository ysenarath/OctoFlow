from octoflow.core.package import Package

package = Package(
    "artifacts",
    modules=[
        {
            "name": ".json_",
            "package": "octoflow.plugins.artifacts",
        },
        {
            "name": ".pandas_",
            "package": "octoflow.plugins.artifacts",
        },
        {
            "name": ".pickle_",
            "package": "octoflow.plugins.artifacts",
        },
        {
            "name": ".transformers_",
            "package": "octoflow.plugins.artifacts",
        },
    ],
)
