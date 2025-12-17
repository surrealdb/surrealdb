#!/bin/bash
# Fix hardcoded library paths in macOS binaries to use @rpath instead
# This prevents issues when binaries are distributed to systems without Homebrew
# or with xz installed in different locations.

set -euo pipefail

BINARY="${1:-target/release/surreal}"

if [[ ! -f "$BINARY" ]]; then
    echo "Error: Binary not found: $BINARY" >&2
    exit 1
fi

if [[ "$(uname)" != "Darwin" ]]; then
    echo "Warning: This script is intended for macOS only" >&2
    exit 0
fi

# Check if install_name_tool is available
if ! command -v install_name_tool &> /dev/null; then
    echo "Error: install_name_tool not found. Please install Xcode Command Line Tools." >&2
    exit 1
fi

# Get all library dependencies
LIBS=$(otool -L "$BINARY" | grep -E "^\s+/opt/homebrew|^\s+/usr/local" | awk '{print $1}' | tr -d ' ')

if [[ -z "$LIBS" ]]; then
    echo "No hardcoded Homebrew/library paths found in $BINARY"
    exit 0
fi

echo "Fixing library paths in $BINARY..."

# Add rpaths to common library locations so @rpath can find libraries
# These are the standard locations where Homebrew and other package managers install libraries
RPATHS=(
    "/opt/homebrew/lib"      # Apple Silicon Homebrew
    "/usr/local/lib"         # Intel Homebrew and other installers
)

for rpath in "${RPATHS[@]}"; do
    # Check if rpath already exists
    if otool -l "$BINARY" | grep -q "path $rpath"; then
        echo "  rpath $rpath already exists"
    else
        echo "  Adding rpath: $rpath"
        install_name_tool -add_rpath "$rpath" "$BINARY" || {
            echo "Warning: Failed to add rpath $rpath" >&2
        }
    fi
done

# Fix each library path to use @rpath
while IFS= read -r lib_path; do
    if [[ -z "$lib_path" ]]; then
        continue
    fi

    # Extract library name (e.g., liblzma.5.dylib from /opt/homebrew/opt/xz/lib/liblzma.5.dylib)
    lib_name=$(basename "$lib_path")

    # Change to @rpath so macOS searches in the rpaths we added above
    # This allows the binary to work on systems with libraries in different locations
    echo "  Changing $lib_path -> @rpath/$lib_name"
    install_name_tool -change "$lib_path" "@rpath/$lib_name" "$BINARY" || {
        echo "Warning: Failed to change $lib_path" >&2
    }
done <<< "$LIBS"

echo "Done fixing library paths"

