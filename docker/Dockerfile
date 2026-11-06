###
# STAGE: builder
# This stage is used to build the SurrealDB linux binary
###

FROM docker.io/rockylinux/rockylinux:9@sha256:8101994123cf3d0a8fee517bee7f39e555c7d92bd2d9eb3303cc988a0eeed00f AS builder

RUN dnf install -y gcc-toolset-13 git cmake llvm-toolset patch zlib-devel python3.11 openssl-devel \
	&& dnf clean all

ENV OPENSSL_NO_VENDOR=1

# Install rust. rustup-init comes from the rustup archive at a fixed version
# and is checked against that version's published checksum for this
# architecture, so the pin holds until RUSTUP_VERSION is changed.
ARG RUST_VERSION=1.95.0
ARG RUSTUP_VERSION=1.29.3
ARG RUSTUP_INIT_SHA256_X86_64=b1c8cb14e30889e7e2999e6b69e62d9e9b9ffc0f4df8180564d306d9680e6c55
ARG RUSTUP_INIT_SHA256_AARCH64=9f608c142e0e38a78957d0661ba1dfaf0b6fb37184cfaa4b9a0aa57af6184282
RUN arch="$(uname -m)" \
    && case "${arch}" in \
        x86_64) sha="${RUSTUP_INIT_SHA256_X86_64}" ;; \
        aarch64) sha="${RUSTUP_INIT_SHA256_AARCH64}" ;; \
        *) echo "no rustup-init checksum for ${arch}" >&2; exit 1 ;; \
    esac \
    && curl --proto '=https' --tlsv1.2 -sSf --retry 3 --retry-connrefused "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${arch}-unknown-linux-gnu/rustup-init" -o /tmp/rustup-init \
    && echo "${sha}  /tmp/rustup-init" | sha256sum -c - \
    && chmod +x /tmp/rustup-init \
    && /tmp/rustup-init -y --default-toolchain ${RUST_VERSION} \
    && rm /tmp/rustup-init
ENV PATH="/root/.cargo/bin:${PATH}"

RUN rustup target add x86_64-unknown-linux-gnu
RUN rustup target add aarch64-unknown-linux-gnu

ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/opt/rh/gcc-toolset-13/root/usr/bin/aarch64-redhat-linux-gcc

WORKDIR /surrealdb

COPY docker/builder-entrypoint.sh /builder-entrypoint.sh

RUN chmod +x /builder-entrypoint.sh

ENTRYPOINT ["/builder-entrypoint.sh"]

###
# STAGE: tzdata
# This stage is used to install the timezone files
###

FROM cgr.dev/chainguard/wolfi-base:latest@sha256:e161445c05b19e668cb5cc44df2f0403329fd4f0ac892794255e328e760612a1 AS tzdata

RUN apk add --no-cache tzdata

###
# Final Images
###

#
# Development image (built on the CI environment)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest-dev@sha256:051877ff31a3c83c704ebcf341d759058bbfd7e102d57883b8abb53fe0461574 AS dev-ci

ARG TARGETOS
ARG TARGETARCH

ARG ARTIFACT_PREFIX

USER root

COPY ${ARTIFACT_PREFIX}.${TARGETOS}-${TARGETARCH}/surreal /surreal

COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=tzdata /usr/share/zoneinfo/UTC /etc/localtime

RUN chmod +x /surreal

RUN mkdir /data /logs \
	&& chown -R nonroot:nonroot /data \
	&& chmod -R 777 /data \
	&& chown -R nonroot:nonroot /logs \
	&& chmod -R 777 /logs \
	&& echo "OK"

USER nonroot

VOLUME /data /logs

ENV SURREAL_BIND="0.0.0.0:8000"

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
	CMD ["/surreal", "isready", "--endpoint", "http://localhost:8000"]

ENTRYPOINT ["/surreal"]

#
# Production image (built on the CI environment)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest@sha256:3f1283dc2a6a48c25cd2e1c7730a871b19b721975240e108e0d6a73b555a3a20 AS prod-ci

COPY --from=dev-ci /surreal /surreal

COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=tzdata /usr/share/zoneinfo/UTC /etc/localtime

# Chainguard's `nonroot` is uid/gid 65532. COPY creates the destination directory as
# root, so the owner is set explicitly or the volume root is unwritable by the runtime user.
COPY --from=dev-ci --chown=65532:65532 /data /data

COPY --from=dev-ci --chown=65532:65532 /logs /logs

USER nonroot

VOLUME /data /logs

ENV SURREAL_BIND="0.0.0.0:8000"

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
	CMD ["/surreal", "isready", "--endpoint", "http://localhost:8000"]

ENTRYPOINT ["/surreal"]

#
# Development image (built locally)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest-dev@sha256:051877ff31a3c83c704ebcf341d759058bbfd7e102d57883b8abb53fe0461574 AS dev

ARG SURREALDB_BINARY=target/release/surreal

COPY ${SURREALDB_BINARY} /surreal

COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=tzdata /usr/share/zoneinfo/UTC /etc/localtime

USER root

RUN chmod +x /surreal

RUN mkdir /data /logs \
	&& chown -R nonroot:nonroot /data \
	&& chmod -R 777 /data \
	&& chown -R nonroot:nonroot /logs \
	&& chmod -R 777 /logs \
	&& echo "OK"

USER nonroot

VOLUME /data /logs

ENV SURREAL_BIND="0.0.0.0:8000"

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
	CMD ["/surreal", "isready", "--endpoint", "http://localhost:8000"]

ENTRYPOINT ["/surreal"]

#
# Production image (built locally)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest@sha256:3f1283dc2a6a48c25cd2e1c7730a871b19b721975240e108e0d6a73b555a3a20 AS prod

COPY --from=dev /surreal /surreal

COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=tzdata /usr/share/zoneinfo/UTC /etc/localtime

# Chainguard's `nonroot` is uid/gid 65532. COPY creates the destination directory as
# root, so the owner is set explicitly or the volume root is unwritable by the runtime user.
COPY --from=dev --chown=65532:65532 /data /data

COPY --from=dev --chown=65532:65532 /logs /logs

USER nonroot

VOLUME /data /logs

ENV SURREAL_BIND="0.0.0.0:8000"

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
	CMD ["/surreal", "isready", "--endpoint", "http://localhost:8000"]

ENTRYPOINT ["/surreal"]
