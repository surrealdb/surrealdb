#!/usr/bin/env sh

set -eu

cd "$(dirname "${0}")/.."

echo '-X "github.com/abcum/surreal/util/vers.rev='$(git rev-parse HEAD)'"' \
     '-X "github.com/abcum/surreal/util/vers.time='$(date -u '+%Y/%m/%d %H:%M:%S')'"'
