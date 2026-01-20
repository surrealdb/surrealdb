#!/usr/bin/env bash
#─────────────────────────────────────────────────────────────────────────────
# Ultra-minimal smoke test for SurrealML (Bash edition)
#  • Creates/recreates venv in clients/python/venv
#  • Installs SurrealML from local checkout (with [dev] extras)
#  • Builds wheel + sdist via setup.py with correct plat-name
#  • Loads one .surml file to prove the bindings work
#
# Usage:
#     ./setup_and_load_surml.sh           # auto-detect first *.surml
#     ./setup_and_load_surml.sh path/to/model.surml
#     ./setup_and_load_surml.sh --recreate
#─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

die() { printf "❌  %s\n" "$*" >&2; exit 1; }

#── parse args ───────────────────────────────────────────────────────────────
MODEL=""
RECREATE=0
for arg in "$@"; do
  case "$arg" in
    --recreate)    RECREATE=1 ;;
    *.surml)       MODEL="$(realpath "$arg")" ;;
    *)             die "Unknown argument: $arg" ;;
  esac
done

#── project layout ───────────────────────────────────────────────────────────
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PY_CLIENT="$(realpath "$SCRIPT_DIR/..")"    # …/clients/python
REPO_ROOT="$(realpath "$PY_CLIENT/../..")"
VENV_DIR="$PY_CLIENT/venv"
STASH_DIR="$REPO_ROOT/modules/core/stash"
PY_BIN="$VENV_DIR/bin/python"

#── recreate? ────────────────────────────────────────────────────────────────
if [[ $RECREATE -eq 1 && -d "$VENV_DIR" ]]; then
  echo "🧹  Removing previous virtual-env …"
  rm -rf "$VENV_DIR"
fi

#── make venv if needed ──────────────────────────────────────────────────────
if [[ ! -x "$PY_BIN" ]]; then
  echo "📦  Creating virtual-env …"
  python3 -m venv "$VENV_DIR"
fi

#── upgrade tooling & install package ────────────────────────────────────────
echo "⬆️   Installing SurrealML + dev extras into venv…"
rm -rf ~/surrealml_deps
export LOCAL_BUILD=TRUE
"$PY_BIN" -m pip install --upgrade pip setuptools wheel
"$PY_BIN" -m pip install "$PY_CLIENT"[dev]

#── build wheel + sdist ──────────────────────────────────────────────────────
echo "📦  Building wheel + sdist in $PY_CLIENT/dist…"
cd "$PY_CLIENT"

# detect host OS & ARCH
uname_s="$(uname -s)"
case "$uname_s" in
  Linux)   OS_NAME=linux   ;;
  Darwin)  OS_NAME=darwin  ;;
  MINGW*|MSYS*|CYGWIN*) OS_NAME=win32 ;;
  *) die "Unsupported OS: $uname_s" ;;
esac

uname_m="$(uname -m)"
case "$uname_m" in
  x86_64|amd64) ARCH=x86_64    ;;
  aarch64|arm64) ARCH=arm64    ;;
  *) die "Unsupported ARCH: $uname_m" ;;
esac

# map to manylinux/macosx/windows tag
if   [[ $OS_NAME == linux   && $ARCH == x86_64 ]]; then plat_tag="manylinux2014_x86_64"
elif [[ $OS_NAME == linux   && $ARCH == arm64  ]]; then plat_tag="manylinux2014_aarch64"
elif [[ $OS_NAME == darwin  && $ARCH == x86_64 ]]; then plat_tag="macosx_10_9_x86_64"
elif [[ $OS_NAME == darwin  && $ARCH == arm64  ]]; then plat_tag="macosx_11_0_arm64"
elif [[ $OS_NAME == win32   && $ARCH == x86_64 ]]; then plat_tag="win_amd64"
else
  die "No wheel tag mapping for OS=$OS_NAME ARCH=$ARCH"
fi

echo "🖥  Detected OS=$OS_NAME ARCH=$ARCH → plat-name=$plat_tag"
export TARGET_OS="$OS_NAME"
export TARGET_ARCH="$ARCH"

# build the wheel
"$PY_BIN" setup.py bdist_wheel --plat-name="$plat_tag"
# build the sdist
"$PY_BIN" setup.py sdist
unset LOCAL_BUILD

echo
echo "✅ Artifacts in dist/:"
ls -lah dist

#── locate a .surml file ─────────────────────────────────────────────────────
if [[ -z "$MODEL" ]]; then
  MODEL="$(find "$STASH_DIR" -maxdepth 1 -name '*.surml' | head -n 1 || true)"
  [[ -n "$MODEL" ]] || die "No *.surml files found in $STASH_DIR"
fi
[[ -f "$MODEL" ]] || die "Model not found: $MODEL"
REL_MODEL="${MODEL#$REPO_ROOT/}"
echo "📂  Loading model: $REL_MODEL"

#── inline Python to load it ─────────────────────────────────────────────────
"$PY_BIN" - <<PY
from pathlib import Path
from surrealml import SurMlFile, Engine

model_str = r"$MODEL"
print("   → about to load", model_str)
SurMlFile.load(model_str, engine=Engine.PYTORCH)
print("🎉  Success – model loaded")
PY
