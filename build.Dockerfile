FROM rust:1.67.0-bullseye as chef
RUN cargo install cargo-chef --locked
WORKDIR /surrealdb

FROM chef as planner
COPY Cargo.lock .
COPY Cargo.toml .
COPY lib/Cargo.toml lib/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder
COPY --from=planner /surrealdb/recipe.json recipe.json
RUN apt-get update && apt-get -y install \
    curl \
    llvm \
    cmake \
    binutils \
    clang-11 \
    qemu-user \
    musl-tools \
    libssl-dev \
    pkg-config \
    build-essential
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release
RUN strip -s ./target/release/surreal

FROM gcr.io/distroless/cc-debian11
COPY --from=builder /surrealdb/target/release/surreal .
ENTRYPOINT ["./surreal"]
