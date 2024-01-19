from typing import Any, Mapping

from octoflow.tracking.artifact.handler import ArtifactHandler

try:
    import numpy as np
    from numpy import ndarray
except ImportError:
    np = None
    ndarray = None

try:
    import torch
    from torch import Tensor as TorchTensor
except ImportError:
    torch = None
    TorchTensor = None

try:
    import tensorflow as tf
    from tensorflow import Tensor as TensorFlowTensor
except ImportError:
    tf = None
    TensorFlowTensor = None

try:
    import safetensors
    from safetensors import safe_open
    from safetensors.numpy import save_file as save_numpy_file
    from safetensors.tensorflow import save_file as save_tensorflow_file
    from safetensors.torch import save_file as save_torch_file
except ImportError:
    safetensors = None
    safe_open = None
    save_numpy_file = None
    save_tensorflow_file = None
    save_torch_file = None


class DictNumpyArrayHandler(ArtifactHandler, name="safetensors.numpy"):
    @classmethod
    def can_handle(cls, obj: Any) -> bool:
        if safetensors is None:
            return False
        if np is None:
            return False
        return isinstance(obj, Mapping) and all(isinstance(t, np.ndarray) for t in obj.values())

    def exists(self) -> bool:
        return (self.path / "data.safetensors").exists()

    def load(self) -> Mapping[str, ndarray]:
        tensors = {}
        with safe_open(self.path / "data.safetensors", framework="np") as f:
            for k in f:
                tensors[k] = f.get_tensor(k)
        return tensors

    def save(self, obj: Mapping[str, ndarray]):
        if save_numpy_file is None:
            msg = "safetensors must be installed"
            raise ImportError(msg)
        if not isinstance(obj, Mapping):
            msg = "obj must be a Mapping"
            raise TypeError(msg)
        if not all(isinstance(t, np.ndarray) for t in obj.values()):
            msg = "obj must be a Mapping of numpy arrays"
            raise TypeError(msg)
        save_numpy_file(obj, self.path / "data.safetensors")


class DictTorchTensorHandler(ArtifactHandler, name="safetensors.torch"):
    @classmethod
    def can_handle(cls, obj: Any) -> bool:
        if safetensors is None:
            return False
        if torch is None:
            return False
        return isinstance(obj, Mapping) and all(isinstance(t, TorchTensor) for t in obj.values())

    def exists(self) -> bool:
        return (self.path / "data.safetensors").exists()

    def load(self) -> Mapping[str, TorchTensor]:
        tensors = {}
        with safe_open(self.path / "data.safetensors", framework="pt") as f:
            for k in f:
                tensors[k] = f.get_tensor(k)
        return tensors

    def save(self, obj: Mapping[str, TorchTensor]):
        if save_torch_file is None:
            msg = "safetensors must be installed"
            raise ImportError(msg)
        if not isinstance(obj, Mapping):
            msg = "obj must be a Mapping"
            raise TypeError(msg)
        if not all(isinstance(t, TorchTensor) for t in obj.values()):
            msg = "obj must be a Mapping of torch tensors"
            raise TypeError(msg)
        save_torch_file(obj, self.path / "data.safetensors")


class DictTensorFlowTensorHandler(ArtifactHandler, name="safetensors.tensorflow"):
    @classmethod
    def can_handle(cls, obj: Any) -> bool:
        if safetensors is None:
            return False
        if tf is None:
            return False
        return isinstance(obj, Mapping) and all(tf.is_tensor(t) for t in obj.values())

    def exists(self) -> bool:
        return (self.path / "data.safetensors").exists()

    def load(self) -> Mapping[str, TensorFlowTensor]:
        tensors = {}
        with safe_open(self.path / "data.safetensors", framework="tf") as f:
            for k in f:
                tensors[k] = f.get_tensor(k)
        return tensors

    def save(self, obj: Mapping[str, TensorFlowTensor]):
        if save_tensorflow_file is None:
            msg = "safetensors must be installed"
            raise ImportError(msg)
        if not isinstance(obj, Mapping):
            msg = "obj must be a Mapping"
            raise TypeError(msg)
        if not all(tf.is_tensor(t) for t in obj.values()):
            msg = "obj must be a Mapping of tensorflow tensors"
            raise TypeError(msg)
        save_tensorflow_file(obj, self.path / "data.safetensors")
