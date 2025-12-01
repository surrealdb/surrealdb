.DEFAULT_GOAL := default

# Ignore Makefile as a target
Makefile:;

# Default target - run the default task
.PHONY: default check-deps
default: check-deps
	@cargo make

# Catch-all - pass any target through to cargo make
%: check-deps
	@cargo make $@

.PHONY: check-deps
check-deps:
	@cargo make --help >/dev/null 2>&1 || { \
		echo >&2 "ERROR: Install cargo-make to use make tasks."; \
		echo >&2 "$ cargo install --no-default-features --force --locked cargo-make"; \
		echo >&2 "More info: https://sagiegurari.github.io/cargo-make"; \
		echo >&2; \
		exit 1; \
	}
