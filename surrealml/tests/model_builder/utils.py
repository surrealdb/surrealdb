import os
import shutil
import subprocess
import sys

import pkg_resources

SCRIPT_PATH = os.path.abspath(__file__)
SCRIPT_DIRECTORY = os.path.dirname(SCRIPT_PATH)

TESTS_DIRECTORY = os.path.join(SCRIPT_DIRECTORY, "..")
MAIN_DIRECTORY = os.path.join(TESTS_DIRECTORY, "..")
CORE_DIRECTORY = os.path.join(MAIN_DIRECTORY, "modules", "core")
MODEL_STASH_DIRECTORY = os.path.join(CORE_DIRECTORY, "model_stash")


def install_package(package_name):
    try:
        # Check if the package is installed
        pkg_resources.require(package_name)
        print(f"{package_name} is already installed.")
    except pkg_resources.DistributionNotFound:
        # If not installed, install the package using pip
        print(f"{package_name} not found, installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        print(f"{package_name} has been installed.")


def delete_directory(dir_path: os.path) -> None:
    """
    Checks to see if a directory exists and deletes it if it does.

    :param dir_path: the path to the directory.
    """
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        print(f"Directory '{dir_path}' has been deleted.")
    else:
        print(f"Directory '{dir_path}' does not exist.")


def delete_file(file_path: os.path) -> None:
    """
    Checks to see if a file exists and deletes it if it does.

    :param file_path: the path to the file.
    """
    if os.path.isfile(file_path):
        os.remove(file_path)
        print(f"File '{file_path}' has been deleted.")
    else:
        print(f"File '{file_path}' does not exist.")


def write_file(file_path: os.path, model, file_name) -> None:
    """
    Writes a file to the specified path.

    :param file_path: the path to write the file to.
    :param model: the model to write to the file.
    :param file_name: the name of the file to write.
    """
    with open(os.path.join(file_path, file_name), "wb") as f:
        f.write(model)


def create_directory(dir_path: os.path) -> None:
    """
    Checks to see if a directory exists and creates it if it does not.

    :param dir_path: the path to the directory.
    """
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
        print(f"Directory '{dir_path}' has been created.")
    else:
        print(f"Directory '{dir_path}' already exists.")
