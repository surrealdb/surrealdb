"""
Defines all the routes for the testing module to all the assets and C libs
"""
from pathlib import Path


UTILS_PATH = Path(__file__).parent
ASSETS_PATH = UTILS_PATH.joinpath("assets")
TEST_SURML_PATH = ASSETS_PATH.joinpath("test.surml")
SHOULD_BREAK_FILE = ASSETS_PATH.joinpath("should_break.txt")
TEST_ONNX_FILE_PATH = ASSETS_PATH.joinpath("linear_test.onnx")
ONNX_LIB = UTILS_PATH.joinpath("..").joinpath("..").joinpath("onnx_lib").joinpath("onnxruntime")
