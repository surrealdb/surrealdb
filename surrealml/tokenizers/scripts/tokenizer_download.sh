#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <HF_TOKEN>"
  echo
  echo "You can also set HF_TOKEN in the environment instead of passing it."
  exit 1
}

# 1) Read token from arg or env
if [[ -n "${1-}" && "$1" != -* ]]; then
  HF_TOKEN="$1"
elif [[ -n "${HF_TOKEN-}" ]]; then
  : # already in env
else
  usage
fi

# 2) Locate script dir and target download folder
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="$SCRIPT_DIR/../tokenizers"
mkdir -p "$TARGET_DIR"

# 3) List of public models
models=(
  "mistralai/Mixtral-8x7B-v0.1"
  "mistralai/Mistral-7B-v0.1"
  "amazon/MistralLite"
  "google/gemma-7b"
  "google/gemma-2b"
  "google/gemma-3-4b-it"
  "tiiuae/falcon-7b"
)

# 4) Download each with your token
for m in "${models[@]}"; do
  fname="${m//\//-}-tokenizer.json"
  url="https://huggingface.co/${m}/resolve/main/tokenizer.json"
  out="$TARGET_DIR/$fname"
  echo "Downloading $m → $out"
  curl -sSfL \
       -H "Authorization: Bearer $HF_TOKEN" \
       "$url" -o "$out"
done

echo "✅ All done! Tokenizers saved in $TARGET_DIR"
