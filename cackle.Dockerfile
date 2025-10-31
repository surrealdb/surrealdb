# Dockerfile for running cargo-acl with Rust nightly
# This image just sets up the environment - mount your codebase at runtime
FROM rust:latest

# Install system dependencies
RUN apt-get update && \
    apt-get install -y bubblewrap clang libclang-dev && \
    rm -rf /var/lib/apt/lists/*

# Install nightly Rust toolchain
RUN rustup toolchain install nightly && \
    rustup default nightly

# Add wasm32-unknown-unknown target for nightly
RUN rustup target add wasm32-unknown-unknown

# Install cargo-acl (cached in this layer)
RUN cargo install --locked cargo-acl

# Set up workspace
WORKDIR /workspace

# Default command: run cargo acl
# Temporarily moves rust-toolchain.toml to use nightly instead of stable
CMD ["sh", "-c", "mv rust-toolchain.toml rust-toolchain.toml.bak 2>/dev/null || true && cargo acl -n; EXIT_CODE=$?; mv rust-toolchain.toml.bak rust-toolchain.toml 2>/dev/null || true; exit $EXIT_CODE"]

