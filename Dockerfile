FROM alpine:latest

RUN apk add --update --no-cache ca-certificates

ADD surrealdb /usr/bin/

ENTRYPOINT ["surrealdb"]
