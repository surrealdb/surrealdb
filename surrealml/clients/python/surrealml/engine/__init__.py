from enum import Enum

from surrealml.engine.sklearn import SklearnOnnxAdapter
from surrealml.engine.torch import TorchOnnxAdapter
from surrealml.engine.tensorflow import TensorflowOnnxAdapter
from surrealml.engine.onnx import OnnxAdapter


class Engine(Enum):
    """
    The Engine enum is used to specify the engine to use for a given model.

    Attributes:
        PYTORCH: The PyTorch engine which will be PyTorch and ONNX.
        NATIVE: The native engine which will be native rust and linfa.
        SKLEARN: The sklearn engine which will be sklearn and ONNX
        TENSOFRLOW: The TensorFlow engine which will be TensorFlow and ONNX
        ONNX: The ONNX engine which bypasses the conversion to ONNX.
    """
    PYTORCH = "pytorch"
    NATIVE = "native"
    SKLEARN = "sklearn"
    TENSORFLOW = "tensorflow"
    ONNX = "onnx"
