from octoflow.logging import get_logger
from octoflow.plugins import artifacts

logger = get_logger(__name__)

__all__ = [
    "artifacts",
    "import_modules",
]


def import_modules():
    artifacts.package.import_modules()
