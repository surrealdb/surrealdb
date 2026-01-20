#!/usr/bin/env bash

# navigate to directory
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
cd $SCRIPTPATH

cd ../..


act -W .github/workflows/surrealml_core_tensorflow_test.yml pull_request