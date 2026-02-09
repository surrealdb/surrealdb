"""
this script simply moves the rust_surrealml.so file from the build directory to the surrealml directory.
This script should be run in the github actions, if you are looking to run the tests locally please run
the local_build.py script.
"""
import fnmatch
import os
import shutil


def find_and_move_rust_surrealml_file(start_path: os.path, destination_path: os.path, new_name: str) -> None:
    """
    Finds the rust_surrealml.so file and moves it to the surrealml directory.

    :param start_path: the path to start the search from for the built .so rust lib.
    :param destination_path: the path to move the rust lib to.
    :param new_name: the new name of the rust lib .so file.
    """
    for root, dirs, files in os.walk(start_path):
        if 'lib' in root:
            for filename in fnmatch.filter(files, 'rust_surrealml*.so'):
                source_file = os.path.join(root, filename)
                destination_file = os.path.join(destination_path, new_name)
                shutil.move(source_file, destination_file)
                return destination_file
    return None


script_path = os.path.abspath(__file__)
script_directory = os.path.dirname(script_path)
main_directory = os.path.join(script_directory, "..", "..")
build_dir = os.path.join(main_directory, "build")
surrealml_dir = os.path.join(main_directory, "surrealml")


def main():
    find_and_move_rust_surrealml_file(
        start_path=build_dir,
        destination_path=surrealml_dir,
        new_name="rust_surrealml.so"
    )


if __name__ == '__main__':
    main()
