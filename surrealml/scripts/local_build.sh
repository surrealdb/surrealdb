#!/usr/bin/env bash

# navigate to directory
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH


delete_directory() {
    dir_path="$1"

    if [ -d "$dir_path" ]; then
        rm -rf "$dir_path"
        echo "Directory '$dir_path' has been deleted."
    else
        echo "Directory '$dir_path' does not exist."
    fi
}

delete_file() {
    file_path="$1"

    if [ -f "$file_path" ]; then
        rm "$file_path"
        echo "File '$file_path' has been deleted."
    else
        echo "File '$file_path' does not exist."
    fi
}


cd ..

delete_directory ./build
delete_directory ./tests/venv
cd tests
python3 -m venv venv
source venv/bin/activate
cd ..
pip install --no-cache-dir .
