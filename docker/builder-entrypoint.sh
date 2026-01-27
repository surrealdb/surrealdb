#!/bin/bash

# Append linker flags to help find static libraries for ONNX Runtime
if [ -n "$RUSTFLAGS" ]; then
    export RUSTFLAGS="$RUSTFLAGS -C link-arg=-ldl -C link-arg=-lm -C link-arg=-lpthread -C link-arg=-lrt"
else
    export RUSTFLAGS="-C link-arg=-ldl -C link-arg=-lm -C link-arg=-lpthread -C link-arg=-lrt"
fi

exec cargo build "$@"
