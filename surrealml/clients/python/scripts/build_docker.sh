#!/usr/bin/env bash

# navigate to directory
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH

cd ..

BUILD_DIR="build-context"

if [ -d "$BUILD_DIR" ]; then
    echo "Cleaning up existing build directory..."
    rm -rf "$BUILD_DIR"
fi

mkdir "$BUILD_DIR"
mkdir "$BUILD_DIR"/clients
mkdir "$BUILD_DIR"/clients/python
mkdir "$BUILD_DIR"/modules
mkdir "$BUILD_DIR"/modules/
mkdir "$BUILD_DIR"/modules/

cp -r surrealml "$BUILD_DIR"/clients/python/surrealml
cp -r assets "$BUILD_DIR"/clients/python/assets
cp setup.py "$BUILD_DIR"/clients/python/setup.py
cp pyproject.toml "$BUILD_DIR"/clients/python/pyproject.toml

cp Dockerfile "$BUILD_DIR"/Dockerfile

cp -r ../../modules/c-wrapper/ "$BUILD_DIR"/modules/
cp -r ../../modules/core/ "$BUILD_DIR"/modules/
rm -rf "$BUILD_DIR"/modules/core/.git
rm -rf "$BUILD_DIR"/modules/c-wrapper/.git
rm -rf "$BUILD_DIR"/modules/core/target/
rm -rf "$BUILD_DIR"/modules/c-wrapper/target/
cd "$BUILD_DIR"
docker build --no-cache -t surrealml-python .

docker run -it surrealml-python /bin/bash