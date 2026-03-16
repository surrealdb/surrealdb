#!/bin/bash
set -e

. /opt/rh/gcc-toolset-13/enable

# When ORT_DOWNLOAD_URL is set, download ONNX Runtime in-container (avoids host mount issues).
if [[ -n "${ORT_DOWNLOAD_URL:-}" ]]; then
  mkdir -p /tmp/onnxruntime-build
  curl -sSL "$ORT_DOWNLOAD_URL" | tar -xz -C /tmp/onnxruntime-build
  export ORT_STRATEGY=system ORT_LIB_LOCATION=/tmp/onnxruntime-build/lib
fi

exec cargo build "$@"
