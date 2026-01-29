#!/bin/bash

. /opt/rh/gcc-toolset-13/enable

# Add system pkg-config paths so openssl-sys can find OpenSSL
export PKG_CONFIG_PATH="/usr/lib64/pkgconfig:/usr/share/pkgconfig:${PKG_CONFIG_PATH}"

exec cargo build "$@"
