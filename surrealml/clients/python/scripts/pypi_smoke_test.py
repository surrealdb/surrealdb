#!/usr/bin/env python3
"""
python/scripts/smoke_install_and_load.py

1. Deletes any existing venv in …/clients/python/venv
2. Creates a fresh venv there
3. pip-installs the published package sm123
4. Checks for libc_wrapper.<ext> inside the installed surrealml package (✅/❌)
5. Loads a .surml model via SurrealML to prove the bindings work
"""

import shutil
import subprocess
import sys
import venv
from pathlib import Path
import platform
import importlib.util

# ── layout ────────────────────────────────────────────────────────────────────
HERE       = Path(__file__).parent    # …/clients/python/scripts
PY_ROOT    = HERE.parent              # …/clients/python
VENV_DIR   = PY_ROOT / "venv"
PY_BIN     = VENV_DIR / ("Scripts/python.exe" if sys.platform.startswith("win") else "bin/python")

# ── 1) (re)create venv ───────────────────────────────────────────────────────
if VENV_DIR.exists():
    print("🧹  Removing old venv…")
    shutil.rmtree(VENV_DIR)
print("📦  Creating new venv…")
venv.EnvBuilder(with_pip=True).create(VENV_DIR)

# ── helper to run inside venv ────────────────────────────────────────────────
def v(cmd):
    subprocess.run([str(PY_BIN)] + cmd, check=True)

# ── 2) install surrealml ─────────────────────────────────────────────────────────
print("⬇️   Installing surrealml")
v(["-m", "pip", "install", "--upgrade", "pip"])
v(["-m", "pip", "install", "surrealml"])

# ── 3) locate native lib ─────────────────────────────────────────────────────
suffix = { "Linux": ".so", "Darwin": ".dylib" }.get(platform.system(), ".dll")

print("🔍  Locating surrealml package inside the venv…")
result = subprocess.run(
    [str(PY_BIN), "-c",
     "import surrealml, pathlib; print(pathlib.Path(surrealml.__file__).parent.as_posix())"],
    cwd=str(VENV_DIR),                      # ← run from inside the venv dir
    capture_output=True, text=True, check=True
)
pkg_dir = Path(result.stdout.strip())
print(f"    → surrealml lives here: {pkg_dir}")

lib_path = pkg_dir / f"libc_wrapper{suffix}"
print(f"    → checking for native lib at: {lib_path}")
if not lib_path.exists():
    sys.exit(f"❌  Missing native lib at {lib_path}")
print("✅  Found native lib")

# ── 4) smoke-load a .surml file ────────────────────────────────────────────────
stash = PY_ROOT.parent.parent / "modules" / "core" / "stash"
models = list(stash.glob("*.surml"))
if not models:
    sys.exit(f"❌  No .surml files in {stash}")
model = models[0]
print(f"📂  Loading model: {model}")

# run a tiny snippet in the venv
code = f"""
from surrealml import SurMlFile, Engine
from surrealml.loader import LibLoader     
SurMlFile.load(r"{model}", engine=Engine.PYTORCH)
print("🎉  Success – model loaded")
LibLoader()  
print("🎉  Success – LibmodelLoader() loaded")
"""
subprocess.run([str(PY_BIN), "-c", code], cwd=str(VENV_DIR), check=True)

print("🏁  Smoke test complete")
