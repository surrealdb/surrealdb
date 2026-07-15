###
# STAGE: builder
# This stage is used to build the SurrealDB linux binary
###

FROM docker.io/rockylinux/rockylinux:9@sha256:8101994123cf3d0a8fee517bee7f39e555c7d92bd2d9eb3303cc988a0eeed00f AS builder

RUN dnf install -y gcc-toolset-13 git cmake llvm-toolset patch zlib-devel python3.11 openssl-devel \
	&& dnf clean all

ENV OPENSSL_NO_VENDOR=1

# Install rust
ARG RUST_VERSION=1.95.0
ARG RUSTUP_SHA256=6c30b75a75b28a96fd913a037c8581b580080b6ee9b8169a3c0feb1af7fe8caf
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh \
    && echo "${RUSTUP_SHA256}  /tmp/rustup.sh" | sha256sum -c - \
    && bash /tmp/rustup.sh -y --default-toolchain ${RUST_VERSION}
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
FROM cgr.dev/chainguard/glibc-dynamic:latest-dev@sha256:6d00a134118686aca478cbf82315be37c38fba5e8608f1f1b215f5dad8cfdfd2 AS dev-ci

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
FROM cgr.dev/chainguard/glibc-dynamic:latest@sha256:0dc86136587f0ac15d61d307dcd8193e4a9880d26d2f2659b9e2b142640eecc0 AS prod-ci

COPY --from=dev-ci /surreal /surreal

COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=tzdata /usr/share/zoneinfo/UTC /etc/localtime

COPY --from=dev-ci /data /data

COPY --from=dev-ci /logs /logs

USER nonroot

VOLUME /data /logs

ENV SURREAL_BIND="0.0.0.0:8000"

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
	CMD ["/surreal", "isready", "--endpoint", "http://localhost:8000"]

ENTRYPOINT ["/surreal"]

#
# Development image (built locally)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest-dev@sha256:6d00a134118686aca478cbf82315be37c38fba5e8608f1f1b215f5dad8cfdfd2 AS dev

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
FROM cgr.dev/chainguard/glibc-dynamic:latest@sha256:0dc86136587f0ac15d61d307dcd8193e4a9880d26d2f2659b9e2b142640eecc0 AS prod

COPY --from=dev /surreal /surreal

COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=tzdata /usr/share/zoneinfo/UTC /etc/localtime

COPY --from=dev /data /data

COPY --from=dev /logs /logs

USER nonroot

VOLUME /data /logs

ENV SURREAL_BIND="0.0.0.0:8000"

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
	CMD ["/surreal", "isready", "--endpoint", "http://localhost:8000"]

ENTRYPOINT ["/surreal"]
