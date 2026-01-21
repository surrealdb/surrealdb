#!/usr/bin/env bash

# navigate to directory
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH

cd ..
OS=$(uname)

# Set the library name and extension based on the OS
case "$OS" in
  "Linux")
    LIB_NAME="libc_wrapper.so"
    ;;
  "Darwin")
    LIB_NAME="libc_wrapper.dylib"
    ;;
  "CYGWIN"*|"MINGW"*)
    LIB_NAME="libc_wrapper.dll"
    ;;
  *)
    echo "Unsupported operating system: $OS"
    exit 1
    ;;
esac

# Source directory (where Cargo outputs the compiled library)
SOURCE_DIR="target/debug"

# Destination directory (tests directory)
DEST_DIR="tests/test_utils"

# Destination directory (onnxruntime library)
LIB_PATH="onnx_lib/onnxruntime"


cp "$SOURCE_DIR/$LIB_NAME" "$DEST_DIR/"
cp "$LIB_PATH" "$DEST_DIR/"
