###
# STAGE: builder
# This stage is used to build the SurrealDB linux binary
###

FROM docker.io/rockylinux/rockylinux:9 AS builder

RUN dnf install -y gcc-toolset-13 git cmake llvm-toolset patch zlib-devel python3.11

# Install rust
ARG RUST_VERSION=1.88.0
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
# STAGE: tzdata
# This stage is used to install the timezone files
###

FROM cgr.dev/chainguard/wolfi-base AS tzdata

RUN apk add --no-cache tzdata

###
# Final Images
###

#
# Development image (built on the CI environment)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest-dev AS dev-ci

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

VOLUME /data /logs

ENV SURREAL_BIND="0.0.0.0:8000"

ENTRYPOINT ["/surreal"]

#
# Production image (built on the CI environment)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest AS prod-ci

COPY --from=dev-ci /surreal /surreal

COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=tzdata /usr/share/zoneinfo/UTC /etc/localtime

COPY --from=dev-ci /data /data

COPY --from=dev-ci /logs /logs

VOLUME /data /logs

ENV SURREAL_BIND="0.0.0.0:8000"

ENTRYPOINT ["/surreal"]

#
# Development image (built locally)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest-dev AS dev

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

VOLUME /data /logs

ENV SURREAL_BIND="0.0.0.0:8000"

ENTRYPOINT ["/surreal"]

#
# Production image (built locally)
#
FROM cgr.dev/chainguard/glibc-dynamic:latest AS prod

COPY --from=dev /surreal /surreal

COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=tzdata /usr/share/zoneinfo/UTC /etc/localtime

COPY --from=dev /data /data

COPY --from=dev /logs /logs

VOLUME /data /logs

ENV SURREAL_BIND="0.0.0.0:8000"

ENTRYPOINT ["/surreal"]
