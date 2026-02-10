#!/usr/bin/env bash

# navigate to directory
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH

cd ..

# compose_command=$1

# docker-compose -f docker-compose.yml -f aarch.yml $1
docker-compose -f docker-compose.yml -f builds/docker_configs/nix.yml $1