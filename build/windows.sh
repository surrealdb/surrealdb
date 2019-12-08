#!/usr/bin/env bash

VERS=$(git describe --tags --abbrev=0)
NAME=surreal-${VERS}.windows-amd64
HASH=${NAME}.txt
FILE=${NAME}.tgz

GOOS=windows GOARCH=amd64 go build -v -ldflags "$(bash build/flags.sh)"
tar -zcvf $FILE -s "#^#${NAME}/#" surreal

echo $(shasum -a 256 $FILE | cut -f1 -d' ') > $HASH

aws s3 cp --region eu-west-2 --cache-control "no-store" ./$FILE s3://download.surrealdb.com/
aws s3 cp --region eu-west-2 --cache-control "no-store" ./$HASH s3://download.surrealdb.com/

rm -rf $FILE $HASH surreal.exe surreal
