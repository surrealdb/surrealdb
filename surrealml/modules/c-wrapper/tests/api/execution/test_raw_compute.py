import ctypes
from unittest import TestCase, main

from test_utils.c_lib_loader import load_library
from test_utils.return_structs import FileInfo, Vecf32Return
from test_utils.routes import TEST_SURML_PATH


class TestExecution(TestCase):

    def setUp(self) -> None:
        self.lib = load_library()

        # Define the Rust function signatures
        self.lib.load_model.argtypes = [ctypes.c_char_p]
        self.lib.load_model.restype = FileInfo

        self.lib.free_file_info.argtypes = [FileInfo]

        self.lib.raw_compute.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_float), ctypes.c_size_t]
        self.lib.raw_compute.restype = Vecf32Return

        self.lib.free_vecf32_return.argtypes = [Vecf32Return]

    def test_raw_compute(self):
        # Load a test model
        c_string = str(TEST_SURML_PATH).encode('utf-8')
        file_info = self.lib.load_model(c_string)

        if file_info.error_message:
            self.fail(f"Failed to load model: {file_info.error_message.decode('utf-8')}")

        # Prepare input data as a ctypes array
        data_buffer = [1.0, 4.0]
        array_type = ctypes.c_float * len(data_buffer)  # Create an array type of the appropriate size
        input_data = array_type(*data_buffer)          # Instantiate the array with the list elements

        # Call the raw_compute function
        result = self.lib.raw_compute(file_info.file_id, input_data, len(input_data))

        if result.is_error:
            self.fail(f"Error in raw_compute: {result.error_message.decode('utf-8')}")

        # Extract and verify the computation result
        outcome = [result.data[i] for i in range(result.length)]
        self.assertEqual(1.8246129751205444, outcome[0])

        # Free allocated memory
        self.lib.free_vecf32_return(result)
        self.lib.free_file_info(file_info)


if __name__ == '__main__':
    main()
