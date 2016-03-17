# Copyright © 2016 Abcum Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GO ?= go
FLG :=
LDF :=
PKG := ./...

# The `make default` command cleans
# the go build and test files and
# then runs a build and install.

.PHONY: default
default: clean build install

# The `make all` command cleans the
# go build and test files, runs all
# tests, and runs a build and install.
# This is used for circleci tests.

.PHONY: all
all: clean test build install

# The `make doc` command runs the godoc
# web gui, for all golang projects in
# the GO src directory

.PHONY: doc
doc:
	godoc -http=:9000

# The `make dev` command downloads or
# updates 'gin', and runs an auto-updating
# development server.

.PHONY: dev
dev:
	$(GO) get -u github.com/codegangsta/gin
	gin -p 3000 -a 33693

# The `make convey` command downloads
# or updates 'goconvey', and runs the
# auto-updating testing server.

.PHONY: convey
convey:
	$(GO) get -u github.com/smartystreets/goconvey
	goconvey -packages 25 -port 5000

# The `make get` command ensures that
# all imported code is up-to-date,
# and also gets and embeds c-rocksdb.

.PHONY: get
get:
	$(GO) get -d ./...
	$(GO) get -tags=embed github.com/tecbot/gorocksdb

# The `make test` command runs all
# tests, found within all sub-folders
# in the project folder.

.PHONY: test
test:
	$(GO) test $(FLG) ./...

# The `make clean` command cleans
# all object, build, and test files
# and removes the executable file.

.PHONY: clean
clean:
	$(GO) clean $(FLG) -i github.com/abcum/...
	find . -name '*.test' -type f -exec rm -f {} \;

# The `make build` command compiles
# the build flags, gets the project
# dependencies, and runs a build.

.PHONY: build
build: LDF += $(shell GOPATH=${GOPATH} build/flags.sh)
build: get
build:
	$(GO) build -v -o surreal $(FLG) -ldflags '$(LDF)'

# The `make install` command compiles
# the build flags, gets the project
# dependencies, and runs an install.

.PHONY: install
install: LDF += $(shell GOPATH=${GOPATH} build/flags.sh)
install: get
install:
	$(GO) install -v $(FLG) -ldflags '$(LDF)'

# The `make cockroach` command ensures
# that cockroachdb is downloads and
# installed, ready for testing.

.PHONY: cockroach
cockroach:
	$(GO) get -d github.com/cockroachdb/cockroach
	$(GO) get -d ../../cockroachdb/cockroach/...
	make -C ../../cockroachdb/cockroach clean
	make -C ../../cockroachdb/cockroach build
	make -C ../../cockroachdb/cockroach install
