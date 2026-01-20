from pathlib import Path
import json, sys


def read_dynamic_lib_version() -> str:
    try:
        cfg_path = Path(__file__).with_name('config.json')

        with cfg_path.open('r') as f:
            cfg = json.load(f)
            return cfg['dynamic_lib_version']
    except Exception as e:
        raise FileNotFoundError(f"Error loading version from '{cfg_path}': {e}")
