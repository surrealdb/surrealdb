FROM gcr.io/distroless/cc:latest

ARG TARGETARCH

ADD $TARGETARCH/surreal /

ENTRYPOINT ["/surreal"]
