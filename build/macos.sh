#!/usr/bin/env bash

VERS=$(git describe --tags --abbrev=0)
NAME=surreal-${VERS}.darwin-universal
HASH=${NAME}.txt
FILE=${NAME}.tgz

GOOS=darwin GOARCH=amd64 go build -v -o surreal-amd64 -ldflags "$(bash build/flags.sh)"
GOOS=darwin GOARCH=arm64 go build -v -o surreal-arm64 -ldflags "$(bash build/flags.sh)"
lipo -create -output surreal surreal-amd64 surreal-arm64
tar -zcvf $FILE -s "#^#${NAME}/#" surreal

echo $(shasum -a 256 $FILE | cut -f1 -d' ') > $HASH

aws s3 cp --region eu-west-2 --cache-control "no-store" ./$FILE s3://download.surrealdb.com/
aws s3 cp --region eu-west-2 --cache-control "no-store" ./$HASH s3://download.surrealdb.com/

rm -rf $FILE $HASH surreal.exe surreal surreal-amd64 surreal-arm64

# amd64

NAME=surreal-${VERS}.darwin-amd64
HASH=${NAME}.txt
FILE=${NAME}.tgz

GOOS=darwin GOARCH=amd64 go build -v -ldflags "$(bash build/flags.sh)"
tar -zcvf $FILE -s "#^#${NAME}/#" surreal

echo $(shasum -a 256 $FILE | cut -f1 -d' ') > $HASH

aws s3 cp --region eu-west-2 --cache-control "no-store" ./$FILE s3://download.surrealdb.com/
aws s3 cp --region eu-west-2 --cache-control "no-store" ./$HASH s3://download.surrealdb.com/

rm -rf $FILE $HASH surreal.exe surreal

# arm64

NAME=surreal-${VERS}.darwin-arm64
HASH=${NAME}.txt
FILE=${NAME}.tgz

GOOS=darwin GOARCH=amd64 go build -v -ldflags "$(bash build/flags.sh)"
tar -zcvf $FILE -s "#^#${NAME}/#" surreal

echo $(shasum -a 256 $FILE | cut -f1 -d' ') > $HASH

aws s3 cp --region eu-west-2 --cache-control "no-store" ./$FILE s3://download.surrealdb.com/
aws s3 cp --region eu-west-2 --cache-control "no-store" ./$HASH s3://download.surrealdb.com/

rm -rf $FILE $HASH surreal.exe surreal
