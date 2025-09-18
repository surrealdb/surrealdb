.PHONY: default
default:
	@echo "Choose a Makefile target:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print "  - " $$1}}' | sort

.PHONY: check-deps
check-deps:
	@cargo make --help >/dev/null 2>&1 || { \
		echo >&2 "ERROR: Install cargo-make to use make tasks."; \
		echo >&2 "$ cargo install --no-default-features --force --locked cargo-make"; \
		echo >&2 "More info: https://sagiegurari.github.io/cargo-make"; \
		echo >&2; \
		exit 1; \
	}

.PHONY: install-linux-deps
install-linux-deps: install-linux-deps
	cd `mktemp -d` && \
	curl -LJO https://github.com/sagiegurari/cargo-make/releases/download/0.37.24/cargo-make-v0.37.24-x86_64-unknown-linux-gnu.zip && \
	unzip -q cargo-make-*.zip && \
	chmod +x cargo-make*/cargo-make && \
	sudo mv cargo-make*/cargo-make /usr/bin/cargo-make && \
	cd -

.PHONY: setup
setup: check-deps
	cargo make setup

.PHONY: docs
docs: check-deps
	cargo make docs

.PHONY: fmt
fmt: check-deps
	cargo make fmt

.PHONY: test
test: check-deps
	cargo make test

.PHONY: check
check: check-deps
	cargo make check

.PHONY: clean
clean: check-deps
	cargo make clean

.PHONY: bench
bench: check-deps
	cargo make bench

.PHONY: serve
serve: check-deps
	cargo make serve

.PHONY: sql
sql: check-deps
	cargo make sql

.PHONY: repl
repl: check-deps
	cargo make repl

.PHONY: build
build: check-deps
	cargo make build

.PHONY: release
release: check-deps
	cargo make release
