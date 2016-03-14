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

.PHONY: default
default: clean test build install

.PHONY: doc
doc:
	godoc -http=:9000

.PHONY: dev
dev:
	gin -p 3000 -a 33693

.PHONY: cover
cover:
	goconvey

.PHONY: clean
clean:
	$(GO) clean $(FLG) -i github.com/abcum/...
	find . -name '*.test' -type f -exec rm -f {} \;

.PHONY: test
test:
	$(GO) test -v $(FLG) ./...

.PHONY: build
build: LDF += $(shell GOPATH=${GOPATH} build/flags.sh)
build:
	$(GO) build -v -o surreal $(FLG) -ldflags '$(LDF)'

.PHONY: install
install: LDF += $(shell GOPATH=${GOPATH} build/flags.sh)
install:
	$(GO) install -v $(FLG) -ldflags '$(LDF)'
