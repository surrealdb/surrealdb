"""
Defines the SurMlFile class which is used to save/load models and perform computations based on those models.
"""
from typing import Optional

from surrealml.engine import Engine, SklearnOnnxAdapter, TorchOnnxAdapter, TensorflowOnnxAdapter, OnnxAdapter
from surrealml.rust_adapter import RustAdapter


class SurMlFile:

    def __init__(self, model=None, name=None, inputs=None, engine=None):
        """
        The constructor for the SurMlFile class.

        :param model: the model to be saved.
        :param name: the name of the model.
        :param inputs: the inputs to the model needed to trace the model so the model can be saved.
        :param sklearn: whether the model is an sklearn model or not.
        """
        self.model = model
        self.name = name
        self.inputs = inputs
        self.engine = engine
        self.file_id = self._cache_model()
        self.rust_adapter = RustAdapter(self.file_id, self.engine)
        # below is optional metadata that can be added to the model through functions of the SurMlFile class
        self.description = None
        self.version = None
        self.author = None

    def _cache_model(self) -> Optional[str]:
        """
        Caches a model, so it can be loaded as raw bytes to be fused with the header.

        :return: the file id of the model so it can be retrieved from the cache.
        """
        # This is triggered when the model is loaded from a file as we are not passing in a model
        if self.model is None and self.name is None and self.inputs is None and self.engine is None:
            return None

        if self.engine == Engine.SKLEARN:
            raw_file_path: str = SklearnOnnxAdapter.save_model_to_onnx(
                model=self.model,
                inputs=self.inputs
            )
        elif self.engine == Engine.PYTORCH:
            raw_file_path: str = TorchOnnxAdapter.save_model_to_onnx(
                model=self.model,
                inputs=self.inputs
            )
        elif self.engine == Engine.TENSORFLOW:
            raw_file_path: str = TensorflowOnnxAdapter.save_model_to_onnx(
                model=self.model,
                inputs=self.inputs
            )
        # Below doesn't really convert to ONNX, but I want to keep the same structure as the other engines
        # (maxwell flitton)
        elif self.engine == Engine.ONNX:
            raw_file_path: str = OnnxAdapter.save_model_to_onnx(
                model=self.model,
                inputs=self.inputs
            )
        else:
            raise ValueError(f"Engine {self.engine} not supported")
        return RustAdapter.pass_raw_model_into_rust(raw_file_path)

    def add_column(self, name):
        """
        Adds a column to the model to the metadata (this needs to be called in order of the columns).

        :param name: the name of the column.
        :return: None
        """
        self.rust_adapter.add_column(name=name)

    def add_output(self, output_name, normaliser_type, one, two):
        """
        Adds an output to the model to the metadata.
        :param output_name: the name of the output.
        :param normaliser_type: the type of normaliser to use.
        :param one: the first parameter of the normaliser.
        :param two: the second parameter of the normaliser.
        :return: None
        """
        self.rust_adapter.add_output(output_name, normaliser_type, one, two)

    def add_description(self, description: str) -> None:
        """
        Adds a description to the model to the metadata.

        :param description: the description of the model.
        :return: None
        """
        self.description = description
        self.rust_adapter.add_description(description)

    def add_version(self, version: str) -> None:
        """
        Adds a version to the model to the metadata.

        :param version: the version of the model.
        :return: None
        """
        self.version = version
        self.rust_adapter.add_version(version)

    def add_name(self, name: str) -> None:
        """
        Adds a name to th model to the metadata.

        :param name: the name of the model.
        :return: None
        """
        self.name = name
        self.rust_adapter.add_name(name)

    def add_normaliser(self, column_name, normaliser_type, one, two):
        """
        Adds a normaliser to the model to the metadata for a column.

        :param column_name: the name of the column (column already needs to be in the metadata to create mapping)
        :param normaliser_type: the type of normaliser to use.
        :param one: the first parameter of the normaliser.
        :param two: the second parameter of the normaliser.
        :return: None
        """
        self.rust_adapter.add_normaliser(column_name, normaliser_type, one, two)

    def add_author(self, author):
        """
        Adds an author to the model to the metadata.

        :param author: the author of the model.
        :return: None
        """
        self.rust_adapter.add_author(author)

    def save(self, path):
        """
        Saves the model to a file.

        :param path: the path to save the model to.
        :return: None
        """
        # right now the only engine is pytorch so we can hardcode it but when we add more engines we will need to
        # add a parameter to the save function to specify the engine
        self.rust_adapter.save(path=path, name=self.name)

    def to_bytes(self):
        """
        Converts the model to bytes.

        :return: the model as bytes.
        """
        return self.rust_adapter.to_bytes()

    @staticmethod
    def load(path, engine: Engine):
        """
        Loads a model from a file so compute operations can be done.

        :param path: the path to load the model from.
        :param engine: the engine to use to load the model.

        :return: The SurMlFile with loaded model and engine definition
        """
        self = SurMlFile()
        self.file_id, self.name, self.description, self.version = self.rust_adapter.load(path)
        self.engine = engine
        self.rust_adapter = RustAdapter(self.file_id, self.engine)
        return self
    
    @staticmethod
    def upload(
            path: str,
            url: str,
            chunk_size: int,
            namespace: str,
            database: str,
            username: Optional[str] = None,
            password: Optional[str] = None
        ) -> None:
        """
        Uploads a model to a remote server.

        :param path: the path to load the model from.
        :param url: the url of the remote server.
        :param chunk_size: the size of each chunk to upload.
        :param namespace: the namespace of the remote server.
        :param database: the database of the remote server.
        :param username: the username of the remote server.
        :param password: the password of the remote server.

        :return: None
        """
        RustAdapter.upload(
            path,
            url,
            chunk_size,
            namespace,
            database,
            username,
            password
        )

    def raw_compute(self, input_vector, dims=None):
        """
        Calculates an output from the model given an input vector.

        :param input_vector: a 1D vector of inputs to the model.
        :param dims: the dimensions of the input vector to be sliced into
        :return: the output of the model.
        """
        return self.rust_adapter.raw_compute(input_vector, dims)

    def buffered_compute(self, value_map):
        """
        Calculates an output from the model given a value map.

        :param value_map: a dictionary of inputs to the model with the column names as keys and floats as values.
        :return: the output of the model.
        """
        return self.rust_adapter.buffered_compute(value_map)
