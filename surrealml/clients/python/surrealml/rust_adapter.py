"""
The adapter to interact with the Rust module compiled to a C dynamic library
"""
import ctypes
import platform
import warnings
from pathlib import Path
from typing import List, Tuple
from typing import Optional

from surrealml.c_structs import EmptyReturn, StringReturn, Vecf32Return, FileInfo, VecU8Return
from surrealml.engine import Engine
from surrealml.loader import LibLoader


class RustAdapter:

    def __init__(self, file_id: str, engine: Engine) -> None:
        self.file_id: str = file_id
        self.engine: Engine = engine
        self.loader = LibLoader()

    @staticmethod
    def pass_raw_model_into_rust(file_path: str) -> str:
        """
        Points to a raw ONNX file and passes it into the rust library so it can be loaded
        and tagged with a unique id so the Rust library can reference this model again
        from within the rust library.

        :param file_path: the path to the raw ONNX file.

        :return: the unique id of the model.
        """
        c_path = file_path.encode("utf-8")
        loader = LibLoader()
        outcome: StringReturn = loader.lib.load_cached_raw_model(c_path)
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        file_path = outcome.string.decode("utf-8")
        loader.lib.free_string_return(outcome)
        return file_path

    def add_column(self, name: str) -> None:
        """
        Adds a column to the model to the metadata (this needs to be called in order of the columns).

        :param name: the name of the column.
        :return: None
        """
        outcome: EmptyReturn = self.loader.lib.add_column(
            self.file_id.encode("utf-8"),
            name.encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        self.loader.lib.free_empty_return(outcome)

    def add_output(self, output_name: str, normaliser_type: str, one: float, two: float) -> None:
        """
        Adds an output to the model to the metadata.
        :param output_name: the name of the output.
        :param normaliser_type: the type of normaliser to use.
        :param one: the first parameter of the normaliser.
        :param two: the second parameter of the normaliser.
        :return: None
        """
        outcome: EmptyReturn = self.loader.lib.add_output(
            self.file_id.encode("utf-8"),
            output_name.encode("utf-8"),
            normaliser_type.encode("utf-8"),
            str(one).encode("utf-8"),
            str(two).encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        self.loader.lib.free_empty_return(outcome)

    def add_description(self, description: str) -> None:
        """
        Adds a description to the model to the metadata.

        :param description: the description of the model.
        :return: None
        """
        outcome: EmptyReturn = self.loader.lib.add_description(
            self.file_id.encode("utf-8"),
            description.encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        self.loader.lib.free_empty_return(outcome)

    def add_version(self, version: str) -> None:
        """
        Adds a version to the model to the metadata.

        :param version: the version of the model.
        :return: None
        """
        outcome: EmptyReturn = self.loader.lib.add_version(
            self.file_id.encode("utf-8"),
            version.encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        self.loader.lib.free_empty_return(outcome)

    def add_name(self, name: str) -> None:
        """
        Adds a name to the model to the metadata.

        :param name: the version of the model.
        :return: None
        """
        outcome: EmptyReturn = self.loader.lib.add_name(
            self.file_id.encode("utf-8"),
            name.encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        self.loader.lib.free_empty_return(outcome)

    def add_normaliser(self, column_name, normaliser_type, one, two) -> None:
        """
        Adds a normaliser to the model to the metadata for a column.

        :param column_name: the name of the column (column already needs to be in the metadata to create mapping)
        :param normaliser_type: the type of normaliser to use.
        :param one: the first parameter of the normaliser.
        :param two: the second parameter of the normaliser.
        :return: None
        """
        outcome: EmptyReturn = self.loader.lib.add_normaliser(
            self.file_id.encode("utf-8"),
            column_name.encode("utf-8"),
            normaliser_type.encode("utf-8"),
            str(one).encode("utf-8"),
            str(two).encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        self.loader.lib.free_empty_return(outcome)

    def add_author(self, author: str) -> None:
        """
        Adds an author to the model to the metadata.

        :param author: the author of the model.
        :return: None
        """
        outcome: EmptyReturn = self.loader.lib.add_author(
            self.file_id.encode("utf-8"),
            author.encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        self.loader.lib.free_empty_return(outcome)

    def save(self, path: str, name: Optional[str]) -> None:
        """
        Saves the model to a file.

        :param path: the path to save the model to.
        :param name: the name of the model.

        :return: None
        """
        outcome: EmptyReturn = self.loader.lib.add_engine(
            self.file_id.encode("utf-8"),
            self.engine.value.encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        self.loader.lib.free_empty_return(outcome)
        outcome: EmptyReturn = self.loader.lib.add_origin(
            self.file_id.encode("utf-8"),
            "local".encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        self.loader.lib.free_empty_return(outcome)
        if name is not None:
            outcome: EmptyReturn = self.loader.lib.add_name(
                self.file_id.encode("utf-8"),
                name.encode("utf-8"),
            )
            if outcome.is_error == 1:
                raise RuntimeError(outcome.error_message.decode("utf-8"))
            self.loader.lib.free_empty_return(outcome)
        else:
            warnings.warn(
                "You are saving a model without a name, you will not be able to upload this model to the database"
            )
        outcome: EmptyReturn = self.loader.lib.save_model(
            path.encode("utf-8"),
            self.file_id.encode("utf-8")
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        self.loader.lib.free_empty_return(outcome)

    def to_bytes(self) -> bytes:
        """
        Converts the model to bytes.

        :return: the model as bytes.
        """
        outcome: VecU8Return = self.loader.lib.to_bytes(
            self.file_id.encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        byte_vec = outcome.data
        self.loader.lib.free_vec_u8(outcome)
        return byte_vec

    @staticmethod
    def load(path) -> Tuple[str, str, str, str]:
        """
        Loads a model from a file.

        :param path: the path to load the model from.
        :return: the id of the model being loaded.
        """
        loader = LibLoader()
        outcome: FileInfo = loader.lib.load_model(
            path.encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        package = (
            outcome.file_id.decode("utf-8"),
            outcome.name.decode("utf-8"),
            outcome.description.decode("utf-8"),
            outcome.version.decode("utf-8"),
        )
        loader.lib.free_file_info(outcome)
        return package

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
        loader: EmptyReturn = LibLoader()
        outcome = loader.lib.upload_model(
            path.encode("utf-8"),
            url.encode("utf-8"),
            chunk_size,
            namespace.encode("utf-8"),
            database.encode("utf-8"),
            username.encode("utf-8"),
            password.encode("utf-8"),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        loader.lib.free_empty_return(outcome)

    def raw_compute(self, input_vector, dims=None) -> List[float]:
        """
        Calculates an output from the model given an input vector.

        :param input_vector: a 1D vector of inputs to the model.
        :param dims: the dimensions of the input vector to be sliced into
        :return: the output of the model.
        """
        array_type = ctypes.c_float * len(input_vector)
        input_data = array_type(*input_vector)
        outcome: Vecf32Return = self.loader.lib.raw_compute(
            self.file_id.encode("utf-8"),
            input_data,
            len(input_data),
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        package = [outcome.data[i] for i in range(outcome.length)]
        self.loader.lib.free_vecf32_return(outcome)
        return package

    def buffered_compute(self, value_map: dict) -> List[float]:
        """
        Calculates an output from the model given a value map.

        :param value_map: a dictionary of inputs to the model with the column names as keys and floats as values.
        :return: the output of the model.
        """
        string_buffer = []
        data_buffer = []
        for key, value in value_map.items():
            string_buffer.append(key.encode('utf-8'))
            data_buffer.append(value)

        # Prepare input data as a ctypes array
        array_type = ctypes.c_float * len(data_buffer)  # Create an array type of the appropriate size
        input_data = array_type(*data_buffer)  # Instantiate the array with the list elements

        # prepare the input strings
        string_array = (ctypes.c_char_p * len(string_buffer))(*string_buffer)
        string_count = len(string_buffer)

        outcome = self.loader.lib.buffered_compute(
            self.file_id.encode("utf-8"),
            input_data,
            len(input_data),
            string_array,
            string_count
        )
        if outcome.is_error == 1:
            raise RuntimeError(outcome.error_message.decode("utf-8"))
        return_data = [outcome.data[i] for i in range(outcome.length)]
        self.loader.lib.free_vecf32_return(outcome)
        return return_data
