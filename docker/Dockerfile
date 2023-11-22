#
# Dockerfile that builds a SurrealDB docker image.
#

FROM cgr.dev/chainguard/rust:latest-dev as builder

USER root
RUN apk update
RUN apk add patch clang curl gcc

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-unknown-linux-gnu-gcc

RUN cargo install cargo-make

RUN mkdir /surrealdb
WORKDIR /surrealdb
COPY . /surrealdb/

RUN cargo build  --features http-compression,storage-tikv --release --locked

#
# Development image
#
FROM cgr.dev/chainguard/glibc-dynamic:latest-dev as dev

USER root
COPY --from=builder /surrealdb/target/release/surreal /surreal

ENTRYPOINT ["/surreal"]

#
# Production image
#
FROM cgr.dev/chainguard/glibc-dynamic:latest as prod

COPY --from=builder /surrealdb/target/release/surreal /surreal

ENTRYPOINT ["/surreal"]
