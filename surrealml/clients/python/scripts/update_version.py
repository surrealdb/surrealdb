#!/usr/bin/env python3
import json
import sys
import argparse
import re
from pathlib import Path


# ─── Constants ──────────────────────────────────────────────────────────────
SEMVER_RE = re.compile(r'^\d+\.\d+\.\d+$')
CONFIG_JSON_PATH = Path(__file__).parent.parent / "config.json"

# ─── Helpers ────────────────────────────────────────────────────────────────
def semver_type(value: str) -> str:
    if not SEMVER_RE.match(value):
        raise argparse.ArgumentTypeError(
            f"Invalid version '{value}': must be in format X.Y.Z"
        )
    return value

def parse_args():
    p = argparse.ArgumentParser(
        description="Bump package version (and optionally dynamic-lib version) "
                    "inside config.json")
    p.add_argument("new_version",            type=semver_type,
                   help="new python_package_version (X.Y.Z)")
    p.add_argument("dynamic_lib_version",    type=semver_type, nargs='?',
                   help="optional: new dynamic_lib_version (X.Y.Z)")
    return p.parse_args()

def load_config() -> dict:
    try:
        with CONFIG_JSON_PATH.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading '{CONFIG_JSON_PATH}': {e}", file=sys.stderr)
        sys.exit(1)

def write_config(cfg: dict) -> None:
    try:
        with CONFIG_JSON_PATH.open('w', encoding='utf-8') as f:
            json.dump(cfg, f, indent=4)
            f.write('\n')
    except Exception as e:
        print(f"Error writing '{CONFIG_JSON_PATH}': {e}", file=sys.stderr)
        sys.exit(1)

# ─── Main ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    args   = parse_args()
    cfg    = load_config()

    cfg["python_package_version"] = args.new_version
    if args.dynamic_lib_version is not None:
        cfg["dynamic_lib_version"] = args.dynamic_lib_version

    write_config(cfg)
    print("✅  config.json updated")
