"""
This file defines the adapter for the ONNX file format. This adapter does not convert anything as the input
model is already in the ONNX format. It simply saves the model to a file. However, I have added this adapter
to keep the same structure as the other adapters for different engines (maxwell flitton).
"""
from surrealml.engine.utils import create_file_cache_path


class OnnxAdapter:

    @staticmethod
    def save_model_to_onnx(model, inputs) -> str:
        """
        Saves a model to an onnx file.

        :param model: the raw ONNX model to directly save
        :param inputs: the inputs to the model needed to trace the model
        :return: the path to the cache created with a unique id to prevent collisions.
        """
        file_path = create_file_cache_path()

        with open(file_path, "wb") as f:
            f.write(model.SerializeToString())

        return file_path

