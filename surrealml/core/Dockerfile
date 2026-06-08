# Use an official Rust image
FROM rust:1.83-slim@sha256:540c902e99c384163b688bbd8b5b8520e94e7731b27f7bd0eaa56ae1960627ab

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    build-essential \
    libssl-dev \
    pkg-config \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 appuser \
    && chown -R appuser:appuser /usr/local/cargo /usr/local/rustup

WORKDIR /app

COPY --chown=appuser:appuser . .

USER appuser

ENV ORT_LIB_LOCATION=/onnxruntime/lib
ENV LD_LIBRARY_PATH=$ORT_LIB_LOCATION:$LD_LIBRARY_PATH

RUN cargo clean
RUN cargo build --features tensorflow-tests

CMD ["cargo", "test", "--features", "tensorflow-tests"]
