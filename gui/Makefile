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

# The `make default` command cleans
# the go build and test files and
# then runs a build and install.

.PHONY: default
default:
	@echo "Choose a Makefile target:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print "  - " $$1}}' | sort

# The `make test` command runs the
# preconfigured tests found in the
# prject directory.

.PHONY: tests
tests:
	@echo "Tests..."
	npm test

# The `make setup` command installs
# the 3rd party dependencies needed
# to run this package.

.PHONY: setup
setup:
	@echo "Setup..."
	npm install -g bower
	npm install -g ember-cli@2.8.0

# The `make install` command installs
# the 3rd party dependencies needed
# to run this package.

.PHONY: install
install:
	@echo "Installing..."
	npm cache clean && bower cache clean
	rm -rf node_modules bower_components dist tmp
	npm install && bower install

# The `make upgrade` command updates
# ember-cli and runs the ember-cli
# init command.

.PHONY: upgrade
upgrade:
	@echo "Upgrading..."
	npm cache clean && bower cache clean
	rm -rf node_modules bower_components dist tmp
	npm install --save-dev ember-cli@2.8.0
	npm install && bower install
	ember init
