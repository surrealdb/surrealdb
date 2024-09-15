###
# STAGE: builder
# This stage is used to build the SurrealDB linux binary
###

FROM docker.io/rockylinux:8 as builder


RUN yum install -y gcc-toolset-13 git cmake llvm-toolset patch zlib-devel python3.11

# Install rust
ARG RUST_VERSION=1.80.1
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /tmp/rustup.sh
RUN bash /tmp/rustup.sh -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup target add x86_64-unknown-linux-gnu
RUN rustup target add aarch64-unknown-linux-gnu

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/opt/rh/gcc-toolset-13/root/usr/bin/aarch64-redhat-linux-gcc

WORKDIR /surrealdb

COPY docker/builder-entrypoint.sh /builder-entrypoint.sh
RUN chmod +x /builder-entrypoint.sh

ENTRYPOINT ["/builder-entrypoint.sh"]

###
# Final Images
###

#
# Development image (built on the CI environment)
#
FROM --platform=$TARGETPLATFORM cgr.dev/chainguard/glibc-dynamic:latest-dev as dev-ci

ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

ARG ARTIFACT_PREFIX

USER root

COPY ${ARTIFACT_PREFIX}.${TARGETOS}-${TARGETARCH}/surreal /surreal

RUN chmod +x /surreal

# Allow connectivity from the host with bridge networking
# This still requires publishing the port when running the container
ENV SURREAL_BIND="0.0.0.0:8000"

ENTRYPOINT ["/surreal"]

#
# Production image (built on the CI environment)
#
FROM --platform=$TARGETPLATFORM cgr.dev/chainguard/glibc-dynamic:latest as prod-ci

ARG TARGETPLATFORM

COPY --from=dev-ci /surreal /surreal

# Allow connectivity from the host with bridge networking
# This still requires publishing the port when running the container
ENV SURREAL_BIND="0.0.0.0:8000"

ENTRYPOINT ["/surreal"]

#
# Development image (built locally)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest-dev as dev

ARG SURREALDB_BINARY=target/release/surreal

COPY ${SURREALDB_BINARY} /surreal

RUN chmod +x /surreal

USER root

# Allow connectivity from the host with bridge networking
# This still requires publishing the port when running the container
ENV SURREAL_BIND="0.0.0.0:8000"

ENTRYPOINT ["/surreal"]

#
# Production image (built locally)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest as prod

COPY --from=dev /surreal /surreal

# Allow connectivity from the host with bridge networking
# This still requires publishing the port when running the container
ENV SURREAL_BIND="0.0.0.0:8000"

ENTRYPOINT ["/surreal"]
