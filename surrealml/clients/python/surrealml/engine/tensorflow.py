import os
import shutil
try:
    import tf2onnx
    import tensorflow as tf
except ImportError:
    tf2onnx = None
    tf = None

from surrealml.engine.utils import TensorflowCache


class TensorflowOnnxAdapter:

    @staticmethod
    def check_dependency() -> None:
        """
        Checks if the tensorflow dependency is installed raising an error if not.
        Please call this function when performing any tensorflow related operations.
        """
        if tf2onnx is None or tf is None:
            raise ImportError("tensorflow feature needs to be installed to use tensorflow features")

    @staticmethod
    def save_model_to_onnx(model, inputs) -> str:
        """
        Saves a tensorflow model to an onnx file.

        :param model: the tensorflow model to convert.
        :param inputs: the inputs to the model needed to trace the model
        :return: the path to the cache created with a unique id to prevent collisions.
        """
        TensorflowOnnxAdapter.check_dependency()
        cache = TensorflowCache()

        model_file_path = cache.new_cache_path
        onnx_file_path = cache.new_cache_path

        tf.saved_model.save(model, model_file_path)

        os.system(
            f"python -m tf2onnx.convert --saved-model {model_file_path} --output {onnx_file_path}"
        )
        shutil.rmtree(model_file_path)
        return onnx_file_path
