DEV_FEATURES ?= storage-mem,http,scripting
SURREAL_LOG ?= trace
SURREAL_USER ?= root
SURREAL_PASS ?= root
SURREAL_AUTH ?= true
SURREAL_PATH ?= memory
SURREAL_NAMESPACE ?= test
SURREAL_DATABASE ?= test

SHELL := env SURREAL_PATH=$(SURREAL_PATH) SURREAL_LOG=$(SURREAL_LOG) SURREAL_AUTH=$(SURREAL_AUTH) SURREAL_USER=$(SURREAL_USER) SURREAL_PASS=$(SURREAL_PASS) $(SHELL)

.PHONY: default
default:
	@echo "Choose a Makefile target:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print "  - " $$1}}' | sort

.PHONY: setup
setup:
	cargo upgrade --pinned
	cargo update

.PHONY: docs
docs:
	cargo doc --open --no-deps --package surrealdb --features rustls,native-tls,protocol-ws,protocol-http,kv-mem,kv-indxdb,kv-speedb,kv-rocksdb,kv-tikv,http,scripting

.PHONY: test
test:
	cargo test --workspace

.PHONY: check
check:
	cargo check --workspace
	cargo fmt --all --check
	cargo fmt --all --check -- ./lib/tests/**/*.rs ./lib/src/kvs/tests/*.rs
	cargo clippy --all-targets --all-features -- -D warnings

.PHONY: clean
clean:
	cargo clean

.PHONY: bench
bench:
	cargo bench --package surrealdb --no-default-features --features kv-mem,http,scripting

.PHONY: serve
serve:
	cargo run --no-default-features --features $(DEV_FEATURES) -- start

.PHONY: sql
sql:
	cargo run --no-default-features --features $(DEV_FEATURES) -- sql --conn ws://0.0.0.0:8000 --multi --pretty

.PHONY: quick
quick:
	cargo build

.PHONY: build
build:
	cargo build --release
