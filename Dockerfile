FROM alpine:latest as builder

RUN apk add --update --no-cache ca-certificates

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ADD surreal /usr/bin/

ENTRYPOINT ["/usr/bin/surreal"]
