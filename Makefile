# Copyright © 2016 SurrealDB Ltd.
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

GO ?= CGO_ENABLED=0 go
CGO ?= CGO_ENABLED=1 go
LDF := -s -w

.PHONY: default
default:
	@echo "Choose a Makefile target:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print "  - " $$1}}' | sort

.PHONY: kill
kill:
	pkill -9 -f surreal

.PHONY: clean
clean:
	$(GO) clean -i -n

.PHONY: tests
tests:
	$(GO) test ./...

.PHONY: racer
racer:
	$(CGO) test -race ./...

.PHONY: build
build: LDF += $(shell GOPATH=${GOPATH} build/flags.sh)
build:
	$(GO) build -v -ldflags '$(LDF)'

.PHONY: install
install: LDF += $(shell GOPATH=${GOPATH} build/flags.sh)
install:
	$(GO) install -v -ldflags '$(LDF)'

.PHONY: compile
compile: LDF += $(shell GOPATH=${GOPATH} build/flags.sh)
compile:
	docker buildx create --use
	GOOS=linux GOARCH=amd64 $(GO) build -v -ldflags '$(LDF)'
	docker build --tag "surrealdb/surrealdb-amd64:latest" .
	GOOS=linux GOARCH=arm64 $(GO) build -v -ldflags '$(LDF)'
	docker build --tag "surrealdb/surrealdb-arm64:latest" .
	docker buildx build --push --pull --cache-from surrealdb/surrealdb-amd64 --cache-from surrealdb/surrealdb-arm64 --build-arg BUILDKIT_INLINE_CACHE=1 --platform linux/amd64,linux/arm64 --tag surrealdb/surrealdb:latest .

.PHONY: deploy
deploy:
	build/macos.sh
	build/linux.sh
	build/windows.sh
