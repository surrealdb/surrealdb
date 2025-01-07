FROM ubuntu:22.04 AS builder
RUN apt-get update && apt-get install -y \
  build-essential \
  git \
  cmake \
  llvm \
  clang \
  zlib1g-dev \
  python3.11 \
  curl

ARG RUST_VERSION=1.80.1
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup.sh
RUN bash /tmp/rustup.sh -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app
COPY . .
RUN cargo build --release

FROM ubuntu:22.04 AS runtime
COPY --from=builder /app/target/release/surreal /usr/local/bin
ENTRYPOINT [ "surreal" ]
