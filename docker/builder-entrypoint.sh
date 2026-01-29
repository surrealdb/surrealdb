#!/bin/bash

. /opt/rh/gcc-toolset-13/enable

# Add system pkg-config paths so openssl-sys can find OpenSSL
export PKG_CONFIG_PATH="/usr/lib64/pkgconfig:/usr/share/pkgconfig:${PKG_CONFIG_PATH}"

# Ensure library paths include both system and gcc-toolset libraries
export LD_LIBRARY_PATH="/opt/rh/gcc-toolset-13/root/usr/lib64:/usr/lib64:${LD_LIBRARY_PATH}"
export LIBRARY_PATH="/opt/rh/gcc-toolset-13/root/usr/lib64:/usr/lib64:${LIBRARY_PATH}"

# Add linker flags for ONNX Runtime static library
export RUSTFLAGS="$RUSTFLAGS -C link-arg=-lstdc++ -C link-arg=-lm -C link-arg=-ldl -C link-arg=-lpthread -C link-arg=-lrt -C link-arg=-latomic"

exec cargo build "$@"
