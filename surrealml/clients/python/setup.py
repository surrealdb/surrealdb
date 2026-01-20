import shutil
import subprocess
from pathlib import Path
import sys
import os
import urllib.request
import tarfile
import zipfile
import platform
import json

from setuptools import setup

# ===================================== Define the paths for the install =================================================

# get the c lib name based on the system
def get_c_lib_name(system: str) -> str:
    if system.lower() == "linux":
        return "libc_wrapper.so"
    elif system.lower() == "darwin":
        return "libc_wrapper.dylib"
    elif system.lower() in ("windows", "win32"):
        return "c_wrapper.dll"
    raise ValueError(f"Unsupported system: {system}")

# Allow CI to override target platform for downloads:
OVERRIDE_OS = os.environ.get("TARGET_OS")
OVERRIDE_ARCH = os.environ.get("TARGET_ARCH")

# save vars about the system
OS_NAME = (OVERRIDE_OS or sys.platform).lower()
ARCH = (OVERRIDE_ARCH or platform.machine()).lower()
SYSTEM = (OVERRIDE_OS or platform.system()).lower()

# define the paths to the C wrapper and root
DIR_PATH = Path(__file__).parent
# path of the root of this github repo
ROOT_PATH = DIR_PATH.joinpath("..").joinpath("..")
# path to where the c-wrapper code for the surrealML core is
C_PATH = ROOT_PATH.joinpath("modules").joinpath("c-wrapper")
# path to where the binary is if it's in c-wrapper
BINARY_PATH = C_PATH.joinpath("target").joinpath("release").joinpath(get_c_lib_name(SYSTEM))
# path to where the binary is if it's in the root
ROOT_BINARY_PATH = ROOT_PATH.joinpath("target").joinpath("release").joinpath(SYSTEM)
# path to where the c-wrapper is stored if held in the surrealML python package
BINARY_DIST = DIR_PATH.joinpath("surrealml").joinpath(get_c_lib_name(SYSTEM))

# ===================================== Version Control =======================================

# copy config.json to the surrealml directory if it doesn't exist
CONFIG_JSON_SRC  = DIR_PATH / "config.json"
CONFIG_JSON_DEST = DIR_PATH / "surrealml" / "config.json"
if not CONFIG_JSON_DEST.exists():    
    shutil.copyfile(CONFIG_JSON_SRC, CONFIG_JSON_DEST)

# get the python package version
CONFIG_JSON_PATH = Path(__file__).parent.joinpath("config.json")

def read_config() -> (str, str):
    try:
        with open(CONFIG_JSON_PATH, "r") as json_data:
            config = json.load(json_data)
            return (config["dynamic_lib_version"], config["python_package_version"])
    except Exception as e:
        print(f"Error loading version from '{CONFIG_JSON_PATH}': {e}", file=sys.stderr)
        sys.exit(1)

# read the version from the config.json file
PYTHON_PACKAGE_VERSION = read_config()[1]
DYNAMIC_LIB_VERSION = read_config()[0]

# ===================================== Necessary Path Extraction =======================================

# Store the root surrealml_deps cache directory
ROOT_DEP_DIR = os.path.expanduser("~/surrealml_deps")

# Ensure the base directory exists
os.makedirs(ROOT_DEP_DIR, exist_ok=True)

# Paths for versioned libraries
DYNAMIC_LIB_DIR = os.path.join(ROOT_DEP_DIR, "core_ml_lib", DYNAMIC_LIB_VERSION)
DYNAMIC_LIB_DIST = DIR_PATH.joinpath(DYNAMIC_LIB_DIR).joinpath(get_c_lib_name(SYSTEM))
DYNAMIC_LIB_DOWNLOAD_CACHE = os.path.join(DYNAMIC_LIB_DIR, "download_cache.tgz")

# create the directories if they don't exist
os.makedirs(DYNAMIC_LIB_DIR, exist_ok=True)

# ===================================== Lib Extraction =======================================

def get_lib_url():
    if OS_NAME.startswith("linux"):
        if ARCH == "x86_64":
            return f"https://github.com/surrealdb/surrealml/releases/download/v{DYNAMIC_LIB_VERSION}/surrealml-v{DYNAMIC_LIB_VERSION}-x86_64-unknown-linux-gnu.tar.gz", f"surrealml-v{DYNAMIC_LIB_VERSION}-x86_64-unknown-linux-gnu.tar.gz"
        elif ARCH in ("arm64", "aarch64"):
            return f"https://github.com/surrealdb/surrealml/releases/download/v{DYNAMIC_LIB_VERSION}/surrealml-v{DYNAMIC_LIB_VERSION}-arm64-unknown-linux-gnu.tar.gz", f"surrealml-v{DYNAMIC_LIB_VERSION}-arm64-unknown-linux-gnu.tar.gz"

    elif OS_NAME == "darwin":
        if ARCH == "x86_64":
            return f"https://github.com/surrealdb/surrealml/releases/download/v{DYNAMIC_LIB_VERSION}/surrealml-v{DYNAMIC_LIB_VERSION}-x86_64-apple-darwin.tar.gz", f"surrealml-v{DYNAMIC_LIB_VERSION}-x86_64-apple-darwin.tar.gz"
        elif ARCH == "arm64":
            return f"https://github.com/surrealdb/surrealml/releases/download/v{DYNAMIC_LIB_VERSION}/surrealml-v{DYNAMIC_LIB_VERSION}-arm64-apple-darwin.tar.gz", f"surrealml-v{DYNAMIC_LIB_VERSION}-arm64-apple-darwin.tar.gz"

    elif OS_NAME == "win32":
        if ARCH == "x86_64":
            return f"https://github.com/surrealdb/surrealml/releases/download/v{DYNAMIC_LIB_VERSION}/surrealml-v{DYNAMIC_LIB_VERSION}-x86_64-pc-windows-msvc.tar.gz", f"surrealml-v{DYNAMIC_LIB_VERSION}-x86_64-pc-windows-msvc.tar.gz"
        elif ARCH == "arm64":
            pass

    raise Exception(f"Unsupported platform or architecture: {OS_NAME}")


def download_and_extract_c_lib():
    # remove any old artifacts
    for child in Path(DYNAMIC_LIB_DIR).iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()

    url, extracted_dir = get_lib_url()
    if not os.path.exists(DYNAMIC_LIB_DOWNLOAD_CACHE):
        print(f"Downloading surrealML lib from {url}")
        urllib.request.urlretrieve(url, DYNAMIC_LIB_DOWNLOAD_CACHE)
    else:
        print(f"the {DYNAMIC_LIB_DOWNLOAD_CACHE} already exists so not downloading surrealML lib")

    if DYNAMIC_LIB_DOWNLOAD_CACHE.endswith(".tgz"):
        with tarfile.open(DYNAMIC_LIB_DOWNLOAD_CACHE, "r:gz") as tar:
            tar.extractall(path=DYNAMIC_LIB_DIR)
    elif DYNAMIC_LIB_DOWNLOAD_CACHE.endswith(".zip"):
        with zipfile.ZipFile(DYNAMIC_LIB_DOWNLOAD_CACHE, "r") as zip_file:
            zip_file.extractall(path=DYNAMIC_LIB_DIR)

    return extracted_dir



# ===================================== Build the rust binary into a dynamic C lib =======================================

BUILD_FLAG = False

# build the C lib and copy it over to the python lib
if DYNAMIC_LIB_DIST.exists() is False and os.environ.get("LOCAL_BUILD") == "TRUE":
    print("building core ML lib locally")
    subprocess.Popen("cargo build --release", cwd=str(C_PATH), shell=True).wait()
    
    if BINARY_PATH.exists():
        shutil.copyfile(BINARY_PATH, DYNAMIC_LIB_DIST)
    elif ROOT_BINARY_PATH.exists():
        shutil.copyfile(ROOT_BINARY_PATH, DYNAMIC_LIB_DIST)

    BUILD_FLAG = True

else:
    print("downloading the core ML lib")
    lib_path = download_and_extract_c_lib()
    os.remove(DYNAMIC_LIB_DOWNLOAD_CACHE)

    # build path to the freshly-extracted library
    downloaded_file = Path(DYNAMIC_LIB_DIR) / get_c_lib_name(SYSTEM)
    if not downloaded_file.exists():
        raise Exception(f"Expected shared lib at {downloaded_file}, but none was found")

    # copy it into the package so package_data will include it
    BINARY_DIST.parent.mkdir(parents=True, exist_ok=True)

    # cleanup existing clibs
    for old in BINARY_DIST.parent.iterdir():
        if old.suffix.lower() in (".so", ".dylib", ".dll"):
            try:
                old.unlink()
            except OSError:
                pass
    
    # copy the new clib into the package
    shutil.copyfile(downloaded_file, BINARY_DIST)

setup(
    name="surrealml",
    version=PYTHON_PACKAGE_VERSION,
    description="A machine learning package for interfacing with various frameworks.",
    author="Maxwell Flitton",
    author_email="maxwellflitton@gmail.com",
    url="https://github.com/surrealdb/surrealml",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "numpy>=2.3",
    ],
    extras_require={
        "dev": [
            "twine",
        ],
        # These ranges intentionally avoid strict pins so pip can solve with NumPy 2.
        # If you want to be stricter, bump the lower bounds once you’ve validated.
        "sklearn": [
            "scikit-learn>=1.5",   # NumPy 2–compatible releases
            "skl2onnx>=1.19.1",
        ],
        "torch": [
            "torch>=2.4",          # reliably NumPy 2–compatible
        ],
        "tensorflow": [
            "tensorflow>=2.16",    # keep loose; TF often pins its own NumPy
            "tf2onnx>=1.16.1",
         ],
    },
    include_package_data=True,   
    zip_safe=False,
    package_data={
        "surrealml": ["*.so", "*.dylib", "*.dll", "config.json"]
    },   
    packages=[
        "surrealml",
        "surrealml.engine",
        "surrealml.model_templates",
        "surrealml.model_templates.datasets",
        "surrealml.model_templates.sklearn",
        "surrealml.model_templates.torch",
        "surrealml.model_templates.tensorflow",
    ],
)
