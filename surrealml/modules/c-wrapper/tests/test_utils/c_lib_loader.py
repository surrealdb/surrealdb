import ctypes
import platform
from pathlib import Path
from test_utils.return_structs import EmptyReturn


def load_library(lib_name: str = "libc_wrapper") -> ctypes.CDLL:
    """
    Load the correct shared library based on the operating system.

    Args:
        lib_name (str): The base name of the library without extension (e.g., "libc_wrapper").

    Returns:
        ctypes.CDLL: The loaded shared library.
    """
    current_dir = Path(__file__).parent
    system_name = platform.system()

    if system_name == "Windows":
        lib_path = current_dir.joinpath(f"{lib_name}.dll")
    elif system_name == "Darwin":  # macOS
        lib_path = current_dir.joinpath(f"{lib_name}.dylib")
    elif system_name == "Linux":
        lib_path = current_dir.joinpath(f"{lib_name}.so")
    else:
        raise OSError(f"Unsupported operating system: {system_name}")
    
    if not lib_path.exists():
        raise FileNotFoundError(f"Shared library not found at: {lib_path}")
    
    loaded_lib = ctypes.CDLL(str(lib_path))
    # loaded_lib.link_onnx.argtypes = []
    loaded_lib.link_onnx.restype = EmptyReturn
    load_info = loaded_lib.link_onnx()
    if load_info.error_message:
        raise OSError(f"Failed to load onnxruntime: {load_info.error_message.decode('utf-8')}")

    return ctypes.CDLL(str(lib_path))
