try:
    import torch
except ImportError:
    torch = None

from surrealml.engine.utils import create_file_cache_path


class TorchOnnxAdapter:

    @staticmethod
    def check_dependency() -> None:
        """
        Checks if the sklearn dependency is installed raising an error if not.
        Please call this function when performing any sklearn related operations.
        """
        if torch is None:
            raise ImportError("torch feature needs to be installed to use torch features")

    @staticmethod
    def save_model_to_onnx(model, inputs) -> str:
        """
        Saves a torch model to an onnx file.

        :param model: the torch model to convert.
        :param inputs: the inputs to the model needed to trace the model
        :return: the path to the cache created with a unique id to prevent collisions.
        """
        # the dynamic import it to prevent the torch dependency from being required for the whole package.
        file_path = create_file_cache_path()
        # below is to satisfy type checkers
        if torch is not None:
            traced_script_module = torch.jit.trace(model, inputs)
            torch.onnx.export(traced_script_module, inputs, file_path)
            return file_path
