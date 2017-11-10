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

GO ?= CGO_ENABLED=0 go
LDF :=

.PHONY: default
default:
	@echo "Choose a Makefile target:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print "  - " $$1}}' | sort

.PHONY: kill
kill:
	pkill -9 -f surreal

.PHONY: clean
clean:
	rm -rf vendor
	$(GO) clean -i -n
	find . -type f \( -name '*.cover' -o -name '*.test' \) -exec rm -f {} \;

.PHONY: setup
setup:
	glide install

.PHONY: tests
tests:
	$(GO) test -v ./...

.PHONY: build
build: LDF += $(shell GOPATH=${GOPATH} build/flags.sh)
build:
	$(GO) build -v -ldflags '$(LDF)'

.PHONY: install
install: LDF += $(shell GOPATH=${GOPATH} build/flags.sh)
install:
	$(GO) install -v -ldflags '$(LDF)'

.PHONY: cover
cover:
	echo 'mode: atomic' > main.cover
	glide novendor | cut -d '/' -f-2 | xargs -I % sh -c 'touch temp.cover; $(GO) test -covermode=count -coverprofile=temp.cover %; tail -n +2 temp.cover >> main.cover; rm temp.cover;'
	goveralls -coverprofile=./main.cover -service=circle-ci -repotoken=${COVERALLS}
