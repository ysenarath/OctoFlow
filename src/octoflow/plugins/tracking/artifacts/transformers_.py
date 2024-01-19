import importlib
import json
from typing import Any, Type, Union

try:
    from transformers import PreTrainedModel, TFPreTrainedModel
except ImportError:
    PreTrainedModel = None
    TFPreTrainedModel = None

from octoflow.tracking.artifact.handler import ArtifactHandler

__all__ = [
    "PreTrainedModelHandler",
]


class PreTrainedModelHandler(ArtifactHandler, name="transformers.PreTrainedModel"):
    @classmethod
    def can_handle(cls, obj: Any) -> bool:
        if PreTrainedModel is None or TFPreTrainedModel is None:
            return False
        return isinstance(obj, (PreTrainedModel, TFPreTrainedModel))

    def exists(self) -> bool:
        model_dir = self.path / "model"
        return (self.path / "task.json").exists() and model_dir.exists() and model_dir.is_dir()

    def load(self) -> Union[PreTrainedModel, TFPreTrainedModel]:
        with open(self.path / "task.json", encoding="utf-8") as f:
            kwargs = json.load(f)
        module = importlib.import_module(name=kwargs["module"])
        model_class: Type[PreTrainedModel] = getattr(module, kwargs["name"])
        return model_class.from_pretrained(self.path / "model")

    def save(self, obj: Union[PreTrainedModel, TFPreTrainedModel]):
        obj.save_pretrained(self.path / "model")
        model_class = type(obj)
        with open(self.path / "task.json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "module": model_class.__module__,
                    "name": model_class.__name__,
                },
                f,
                indent=4,
            )
