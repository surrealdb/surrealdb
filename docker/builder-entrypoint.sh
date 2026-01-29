#!/bin/bash

. /opt/rh/gcc-toolset-13/enable

# Add system pkg-config paths so openssl-sys can find OpenSSL
export PKG_CONFIG_PATH="/usr/lib64/pkgconfig:/usr/share/pkgconfig:${PKG_CONFIG_PATH}"

# Ensure library paths include both system and gcc-toolset libraries for runtime and linking
export LD_LIBRARY_PATH="/opt/rh/gcc-toolset-13/root/usr/lib64:/usr/lib64:${LD_LIBRARY_PATH}"
export LIBRARY_PATH="/opt/rh/gcc-toolset-13/root/usr/lib64:/usr/lib64:${LIBRARY_PATH}"

# Tell the linker where to find libraries (used by ld during linking phase)
export LDFLAGS="-L/opt/rh/gcc-toolset-13/root/usr/lib64 -L/usr/lib64 -Wl,--as-needed"

# Set C++ compiler and ensure it can link C++ programs (for ort-sys build script tests)
export CXX="g++"

# Add linker flags for ONNX Runtime static library
export RUSTFLAGS="$RUSTFLAGS -C link-arg=-lstdc++ -C link-arg=-lm"

exec cargo build "$@"
