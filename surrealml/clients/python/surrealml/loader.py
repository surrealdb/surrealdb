"""
The loader for the dynamic C lib written in Rust.
"""
import ctypes
import platform
from pathlib import Path
import os

from surrealml.c_structs import EmptyReturn, StringReturn, Vecf32Return, FileInfo, VecU8Return
from surrealml.utils import read_dynamic_lib_version

DYNAMIC_LIB_VERSION = read_dynamic_lib_version()


class Singleton(type):
    """
    Ensures that the loader only loads once throughout the program's lifetime
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def load_library(lib_name: str = "libc_wrapper") -> ctypes.CDLL:
    """
    Load the correct shared library based on the operating system.

    Args:
        lib_name (str): The base name of the library without extension (e.g., "libc_wrapper").

    Returns:
        ctypes.CDLL: The loaded shared library.
    """
    system_name = platform.system()

    suffix = {"Linux": ".so", "Darwin": ".dylib", "Windows": ".dll"}.get(system_name)
    if suffix is None:
        raise OSError(f"Unsupported OS: {system_name}")

    lib_file = f"{lib_name}{suffix}"

    # Path inside installed wheel
    pkg_lib_path  = Path(__file__).with_name(lib_file)
    print(pkg_lib_path)

    # Path inside local cache
    cache_root_dir = os.path.expanduser("~/surrealml_deps")
    cache_lib_dir  = Path(cache_root_dir) / "core_ml_lib" / DYNAMIC_LIB_VERSION
    cache_lib_path = cache_lib_dir / lib_file

    pkg_lib_dir = Path(__file__).parent

    for candidate in (pkg_lib_path, cache_lib_path):
        if candidate.exists():
            print(f"candidate chosen - {candidate}")
            return ctypes.CDLL(str(candidate))

    raise FileNotFoundError(
        f"Shared library not found; looked in {pkg_lib_path} and {cache_lib_path}"
    )


def get_onnx_lib_name() -> str:
    system_name = platform.system()
    if system_name == "Windows":
        return "libonnxruntime.dll"
    elif system_name == "Darwin":  # macOS
        return "libonnxruntime.dylib"
    elif system_name == "Linux":
        return "libonnxruntime.so"
    else:
        raise OSError(f"Unsupported operating system: {system_name}")


class LibLoader(metaclass=Singleton):

    def __init__(self, lib_name: str = "libc_wrapper") -> None:
        """
        The constructor for the LibLoader class.

        args:
            lib_name (str): The base name of the library without extension (e.g., "libc_wrapper").
        """
        self.lib = load_library(lib_name=lib_name)
        functions = [
            self.lib.add_name,
            self.lib.add_description,
            self.lib.add_version,
            self.lib.add_column,
            self.lib.add_author,
            self.lib.add_origin,
            self.lib.add_engine,
        ]
        for i in functions:
            i.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
            i.restype = EmptyReturn

        self.lib.add_normaliser.argtypes = [
            ctypes.c_char_p,  # file_id
            ctypes.c_char_p,  # column_name
            ctypes.c_char_p,  # normaliser_type
            ctypes.c_char_p,  # one
            ctypes.c_char_p,  # two
        ]
        self.lib.add_normaliser.restype = EmptyReturn

        # add_output(file_id, output_name, normaliser_type, one, two) -> EmptyReturn (by value)
        self.lib.add_output.argtypes = [
            ctypes.c_char_p,  # file_id
            ctypes.c_char_p,  # output_name
            ctypes.c_char_p,  # normaliser_type
            ctypes.c_char_p,  # one
            ctypes.c_char_p,  # two
        ]
        self.lib.add_output.restype = EmptyReturn

        self.lib.load_model.restype = FileInfo
        self.lib.load_model.argtypes = [ctypes.c_char_p]
        self.lib.load_cached_raw_model.restype = StringReturn
        self.lib.load_cached_raw_model.argtypes = [ctypes.c_char_p]
        self.lib.to_bytes.argtypes = [ctypes.c_char_p]
        self.lib.to_bytes.restype = VecU8Return
        self.lib.save_model.restype = EmptyReturn
        self.lib.save_model.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        self.lib.upload_model.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
        ]
        self.lib.upload_model.restype = EmptyReturn

        # define the compute functions
        self.lib.raw_compute.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_float), ctypes.c_size_t]
        self.lib.raw_compute.restype = Vecf32Return
        self.lib.buffered_compute.argtypes = [
            ctypes.c_char_p,  # file_id_ptr -> *const c_char
            ctypes.POINTER(ctypes.c_float),  # data_ptr -> *const c_float
            ctypes.c_size_t,  # data_length -> usize
            ctypes.POINTER(ctypes.c_char_p),  # strings -> *const *const c_char
            ctypes.c_int  # string_count -> c_int
        ]
        self.lib.buffered_compute.restype = Vecf32Return

        # Define free alloc functions
        self.lib.free_string_return.argtypes = [StringReturn]
        self.lib.free_empty_return.argtypes = [EmptyReturn]
        self.lib.free_vec_u8.argtypes = [VecU8Return]
        self.lib.free_vecf32_return.argtypes = [Vecf32Return]
        self.lib.free_file_info.argtypes = [FileInfo]

        # link the onnx runtime
        self.lib.link_onnx.restype = EmptyReturn
        load_info = self.lib.link_onnx()
        if load_info.error_message:
            raise OSError(f"Failed to load onnxruntime: {load_info.error_message.decode('utf-8')}")
