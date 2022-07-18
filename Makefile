.PHONY: default
default:
	@echo "Choose a Makefile target:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print "  - " $$1}}' | sort

.PHONY: setup
setup:
	cargo upgrade --workspace --pinned --to-lockfile

.PHONY: docs
docs:
	cargo doc --open --no-deps --package surrealdb

.PHONY: test
test:
	cargo test --workspace

.PHONY: check
check:
	cargo check --workspace
	cargo fmt --all -- --check
	cargo clippy -- -W warnings

.PHONY: clean
clean:
	cargo clean

.PHONY: serve
serve:
	cargo run -- -vvv start --user root --pass root memory

.PHONY: quick
quick:
	cargo build

.PHONY: build
build:
	cargo build --release
