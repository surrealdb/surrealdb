FROM alpine:latest

RUN apk add --update --no-cache ca-certificates

ADD surreal /usr/bin/

ENTRYPOINT ["surreal"]
