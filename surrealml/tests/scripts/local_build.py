"""
This script compiles the Rust library and injects the .so rust python lib into the surrealml
directory so we can run python unit tests against the Rust library.
"""
import fnmatch
import os
import shutil


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

tests_directory = os.path.join(script_directory, "..")
main_directory = os.path.join(script_directory, "..", "..")
target_directory = os.path.join(main_directory, "target")
egg_info_dir = os.path.join(main_directory, "surrealml.egg-info")
build_dir = os.path.join(main_directory, "build")
surrealml_dir = os.path.join(main_directory, "surrealml")
embedded_rust_lib_dir = os.path.join(main_directory, "surrealml", "rust_surrealml.so")
test_venv_dir = os.path.join(tests_directory, "venv")
source_venv = os.path.join(test_venv_dir, "bin", "activate")


def main():
    # delete the old dirs and embedded rust lib if present
    print("local build: cleaning up old files")
    delete_directory(dir_path=test_venv_dir)
    delete_directory(dir_path=build_dir)
    delete_directory(dir_path=egg_info_dir)
    delete_directory(dir_path=target_directory)
    delete_file(file_path=embedded_rust_lib_dir)
    print("local build: old files cleaned up")

    # setup venv and build the rust lib
    print("local build: setting up venv and building rust lib")
    os.system(f"python3 -m venv {test_venv_dir}")
    print("local build: venv setup")
    print("local build: building rust lib")
    os.system(f"source {source_venv} && pip install --no-cache-dir {main_directory}")
    print("local build: rust lib built")

    # move the rust lib into the surrealml directory
    print("local build: moving rust lib into surrealml directory")
    find_and_move_rust_surrealml_file(
        start_path=build_dir,
        destination_path=surrealml_dir,
        new_name="rust_surrealml.so"
    )
    print("local build: rust lib moved into surrealml directory")

    # cleanup
    # delete_directory(dir_path=test_venv_dir)
    # delete_directory(dir_path=build_dir)
    # delete_directory(dir_path=egg_info_dir)


if __name__ == '__main__':
    main()
