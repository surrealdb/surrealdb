#!/bin/bash

# Append linker flags for ONNX Runtime (needs C++ stdlib and system libs)
export RUSTFLAGS="$RUSTFLAGS -C link-arg=-lstdc++ -C link-arg=-lm -C link-arg=-ldl -C link-arg=-lpthread"

exec cargo build "$@"
