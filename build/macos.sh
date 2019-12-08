#!/bin/bash

VERS=$(git describe --tags --abbrev=0)
NAME=surreal-${VERS}.darwin-amd64
FILE=${NAME}.tgz

GOOS=darwin GOARCH=amd64 go build -v -ldflags "$(bash build/flags.sh)"
tar -zcvf $FILE -s "#^#${NAME}/#" surreal
aws s3 cp --region eu-west-2 --cache-control "no-store" ./$FILE s3://download.surrealdb.com/
rm -rf $FILE surreal
