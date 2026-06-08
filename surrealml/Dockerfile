# Use an official Rust image
FROM rust:1.88-slim@sha256:38bc5a86d998772d4aec2348656ed21438d20fcdce2795b56ca434cf21430d89

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    build-essential \
    libssl-dev \
    pkg-config \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    vim \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 appuser \
    && chown -R appuser:appuser /usr/local/cargo /usr/local/rustup

WORKDIR /app

COPY --chown=appuser:appuser . .

USER appuser

RUN cargo clean
RUN cargo build
RUN cp ./target/debug/libc_wrapper.so c-wrapper/tests/test_utils/libc_wrapper.so

CMD ["cargo", "test"]
