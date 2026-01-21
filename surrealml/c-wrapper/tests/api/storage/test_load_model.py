import ctypes
from unittest import TestCase, main

from test_utils.c_lib_loader import load_library
from test_utils.return_structs import FileInfo
from test_utils.routes import SHOULD_BREAK_FILE, TEST_SURML_PATH


class TestLoadModel(TestCase):

    def setUp(self) -> None:
        self.lib = load_library()
        self.lib.load_model.restype = FileInfo
        self.lib.load_model.argtypes = [ctypes.c_char_p]
        self.lib.free_file_info.argtypes = [FileInfo]

    def test_null_pointer_protection(self):
        null_pointer = None
        outcome: FileInfo = self.lib.load_model(null_pointer)
        self.assertEqual(1, outcome.is_error)
        self.assertEqual("Received a null pointer for file path", outcome.error_message.decode('utf-8'))

    def test_wrong_file(self):
        wrong_file_type = str(SHOULD_BREAK_FILE).encode('utf-8')
        outcome: FileInfo = self.lib.load_model(wrong_file_type)
        self.assertEqual(1, outcome.is_error)
        self.assertEqual(True, "failed to fill whole buffer" in outcome.error_message.decode('utf-8'))

    def test_success(self):
        surml_file_path = str(TEST_SURML_PATH).encode('utf-8')
        outcome: FileInfo = self.lib.load_model(surml_file_path)
        self.assertEqual(0, outcome.is_error)
        self.lib.free_file_info(outcome)




if __name__ == '__main__':
    main()
