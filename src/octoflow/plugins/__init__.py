from octoflow.logging import get_logger
from octoflow.plugins.package import Package

logger = get_logger(__name__)

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
