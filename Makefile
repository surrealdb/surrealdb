.PHONY: default
default:
	@echo "Choose a Makefile target:"
	@$(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print "  - " $$1}}' | sort

.PHONY: test
test:
	cargo test --workspace

.PHONY: clean
clean:
	cargo clean --workspace

.PHONY: serve
serve:
	cargo run -- -vvv start memory

.PHONY: quick
quick:
	cargo build

.PHONY: build
build:
	cargo build --release
