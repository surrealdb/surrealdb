#!/bin/bash

. /opt/rh/gcc-toolset-13/enable

# Add system pkg-config paths so openssl-sys can find OpenSSL
export PKG_CONFIG_PATH="/usr/lib64/pkgconfig:/usr/share/pkgconfig:${PKG_CONFIG_PATH}"

# Ensure library paths include both system and gcc-toolset libraries
export LD_LIBRARY_PATH="/opt/rh/gcc-toolset-13/root/usr/lib64:/usr/lib64:${LD_LIBRARY_PATH}"
export LIBRARY_PATH="/opt/rh/gcc-toolset-13/root/usr/lib64:/usr/lib64:${LIBRARY_PATH}"

# Set C and C++ compilers explicitly for build scripts
export CC="gcc"
export CXX="g++"

# LDFLAGS: library paths first, then --as-needed, then libs at the END (for ort-sys)
# The --as-needed flag prevents unnecessary dependencies for aws-lc-sys
export LDFLAGS="-L/opt/rh/gcc-toolset-13/root/usr/lib64 -L/usr/lib64 -Wl,--as-needed -lstdc++ -lm"

# Add linker flags for final Rust binary linking
export RUSTFLAGS="$RUSTFLAGS -C link-arg=-lstdc++ -C link-arg=-lm -C link-arg=-ldl"

exec cargo build "$@"
