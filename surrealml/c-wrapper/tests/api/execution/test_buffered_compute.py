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

        self.lib.buffered_compute.argtypes = [
            ctypes.c_char_p,                          # file_id_ptr -> *const c_char
            ctypes.POINTER(ctypes.c_float),           # data_ptr -> *const c_float
            ctypes.c_size_t,                          # data_length -> usize
            ctypes.POINTER(ctypes.c_char_p),          # strings -> *const *const c_char
            ctypes.c_int                              # string_count -> c_int
        ]
        self.lib.buffered_compute.restype = Vecf32Return

        self.lib.free_vecf32_return.argtypes = [Vecf32Return]

    def test_buffered_compute(self):
        # Load a test model
        c_string = str(TEST_SURML_PATH).encode('utf-8')
        file_info = self.lib.load_model(c_string)

        if file_info.error_message:
            self.fail(f"Failed to load model: {file_info.error_message.decode('utf-8')}")

        input_data = {
            "squarefoot": 500.0,
            "num_floors": 2.0
        }

        string_buffer = []
        data_buffer = []
        for key, value in input_data.items():
            string_buffer.append(key.encode('utf-8'))
            data_buffer.append(value)

        # Prepare input data as a ctypes array
        array_type = ctypes.c_float * len(data_buffer)  # Create an array type of the appropriate size
        input_data = array_type(*data_buffer)  # Instantiate the array with the list elements

        # prepare the input strings
        string_array = (ctypes.c_char_p * len(string_buffer))(*string_buffer)
        string_count = len(string_buffer)

        # Call the raw_compute function
        result = self.lib.buffered_compute(
            file_info.file_id,
            input_data,
            len(input_data),
            string_array,
            string_count
        )

        if result.is_error:
            self.fail(f"Error in buffered_compute: {result.error_message.decode('utf-8')}")

        # Extract and verify the computation result
        outcome = [result.data[i] for i in range(result.length)]
        self.assertEqual(362.9851989746094, outcome[0])

        # Free allocated memory
        self.lib.free_vecf32_return(result)
        self.lib.free_file_info(file_info)


if __name__ == '__main__':
    main()
