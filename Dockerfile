FROM alpine:3.5

RUN apk update

RUN apk add --no-cache ca-certificates

ADD app app/

ADD surreal .

EXPOSE 8000 33693

ENTRYPOINT ["/surreal"]
