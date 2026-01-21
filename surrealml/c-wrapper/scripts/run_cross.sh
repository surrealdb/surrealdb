#!/usr/bin/env bash

# navigate to directory
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH

cd ..

dockerd
sudo systemctl start docker
cross build --target aarch64-unknown-linux-gnu


docker run --rm -it \
  -v "$(pwd):/project" \  # Mount the current directory to /project
  -v /var/run/docker.sock:/var/run/docker.sock \  # Share host Docker socket
  -w /project \  # Set the working directory inside the container
  rust-cross-compiler
