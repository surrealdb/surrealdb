#!/bin/bash

. /opt/rh/gcc-toolset-13/enable

exec cargo build "$@"
