"""
Tests all the meta data functions
"""
import ctypes
from unittest import TestCase, main
from typing import Optional
import os

from test_utils.c_lib_loader import load_library
from test_utils.return_structs import EmptyReturn, FileInfo, StringReturn
from test_utils.routes import TEST_SURML_PATH, TEST_ONNX_FILE_PATH, ASSETS_PATH


class TestMeta(TestCase):

    def setUp(self) -> None:
        self.lib = load_library()
        self.lib.add_name.restype = EmptyReturn

        # Define the signatues of the basic meta functions
        self.functions = [
            self.lib.add_name,
            self.lib.add_description,
            self.lib.add_version,
            self.lib.add_column,
            self.lib.add_author,
            self.lib.add_origin,
            self.lib.add_engine,
        ]
        for i in self.functions:
            i.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
            i.restype = EmptyReturn

        # Define the load model signature
        self.lib.load_model.restype = FileInfo
        self.lib.load_model.argtypes = [ctypes.c_char_p]
        self.lib.free_file_info.argtypes = [FileInfo]
        # define the load raw model signature
        self.lib.load_cached_raw_model.restype = StringReturn
        self.lib.load_cached_raw_model.argtypes = [ctypes.c_char_p]
        # define the save model signature
        self.lib.save_model.restype = EmptyReturn
        self.lib.save_model.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        # load the model for tests
        self.model: FileInfo = self.lib.load_model(str(TEST_SURML_PATH).encode('utf-8'))
        self.file_id = self.model.file_id.decode('utf-8')
        self.temp_test_id: Optional[str] = None

    def tearDown(self) -> None:
        self.lib.free_file_info(self.model)

        # remove the temp surml file created in assets if present
        if self.test_temp_surml_file_path is not None:
            os.remove(self.test_temp_surml_file_path)

    def test_null_protection(self):
        placeholder = "placeholder".encode('utf-8')
        file_id = self.file_id.encode('utf-8')

        # check that they all protect against file ID null pointers
        for i in self.functions:
            outcome: EmptyReturn = i(None, placeholder)
            self.assertEqual(1, outcome.is_error)
            self.assertEqual(
                "Received a null pointer for file id",
                outcome.error_message.decode('utf-8')
            )

        # check that they all protect against null pointers for the field type
        outcomes = [
            "model name",
            "description",
            "version",
            "column name",
            "author",
            "origin",
            "engine",
        ]
        counter = 0
        for i in self.functions:
            outcome: EmptyReturn = i(file_id, None)
            self.assertEqual(1, outcome.is_error)
            self.assertEqual(
                f"Received a null pointer for {outcomes[counter]}",
                outcome.error_message.decode('utf-8')
            )
            counter += 1

    def test_model_not_found(self):
        placeholder = "placeholder".encode('utf-8')

        # check they all return errors if not found
        for i in self.functions:
            outcome: EmptyReturn = i(placeholder, placeholder)
            self.assertEqual(1, outcome.is_error)
            self.assertEqual("Model not found", outcome.error_message.decode('utf-8'))

    def test_add_metadata_and_save(self):
        file_id: StringReturn = self.lib.load_cached_raw_model(str(TEST_SURML_PATH).encode('utf-8'))
        self.assertEqual(0, file_id.is_error)

        decoded_file_id = file_id.string.decode('utf-8')
        self.temp_test_id = decoded_file_id

        self.assertEqual(
            0,
            self.lib.add_name(file_id.string, "test name".encode('utf-8')).is_error
        )
        self.assertEqual(
            0,
            self.lib.add_description(file_id.string, "test description".encode('utf-8')).is_error
        )
        self.assertEqual(
            0,
            self.lib.add_version(file_id.string, "0.0.1".encode('utf-8')).is_error
        )
        self.assertEqual(
            0,
            self.lib.add_author(file_id.string, "test author".encode('utf-8')).is_error
        )
        self.assertEqual(
            0,
            self.lib.save_model(self.test_temp_surml_file_path.encode("utf-8"), file_id.string).is_error
        )

        outcome: FileInfo = self.lib.load_model(self.test_temp_surml_file_path.encode('utf-8'))
        self.assertEqual(0, outcome.is_error)
        self.assertEqual("test name", outcome.name.decode('utf-8'))
        self.assertEqual("test description", outcome.description.decode('utf-8'))
        self.assertEqual("0.0.1", outcome.version.decode('utf-8'))


    @property
    def test_temp_surml_file_path(self) -> Optional[str]:
        if self.temp_test_id is None:
            return None
        return str(ASSETS_PATH.joinpath(f"{self.temp_test_id}.surml"))



if __name__ == '__main__':
    main()
