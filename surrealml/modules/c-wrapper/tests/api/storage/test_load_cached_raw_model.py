import ctypes
from unittest import TestCase, main

from test_utils.c_lib_loader import load_library
from test_utils.return_structs import StringReturn
from test_utils.routes import SHOULD_BREAK_FILE, TEST_ONNX_FILE_PATH


class TestLoadCachedRawModel(TestCase):

    def setUp(self) -> None:
        self.lib = load_library()
        # define the types
        self.lib.load_cached_raw_model.restype = StringReturn
        self.lib.load_cached_raw_model.argtypes = [ctypes.c_char_p]

    def test_null_pointer_protection(self):
        null_pointer = None
        outcome: StringReturn = self.lib.load_cached_raw_model(null_pointer)
        self.assertEqual(1, outcome.is_error)
        self.assertEqual("Received a null pointer for file path", outcome.error_message.decode('utf-8'))

    def test_wrong_path(self):
        wrong_path = "should_break".encode('utf-8')
        outcome: StringReturn = self.lib.load_cached_raw_model(wrong_path)
        self.assertEqual(1, outcome.is_error)
        self.assertEqual(
            "No such file or directory (os error 2)",
            outcome.error_message.decode('utf-8')
        )

    def test_wrong_file_format(self):
        wrong_file_type = str(SHOULD_BREAK_FILE).encode('utf-8')
        outcome: StringReturn = self.lib.load_cached_raw_model(wrong_file_type)
        # below is unexpected and also happens in the old API
        # TODO => throw an error if the file format is incorrect
        self.assertEqual(0, outcome.is_error)

    def test_success(self):
        right_file = str(TEST_ONNX_FILE_PATH).encode('utf-8')
        outcome: StringReturn = self.lib.load_cached_raw_model(right_file)
        self.assertEqual(0, outcome.is_error)


if __name__ == '__main__':
    main()
