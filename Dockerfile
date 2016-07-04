FROM alpine:3.4

RUN apk add --no-cache ca-certificates

ADD gui gui/
ADD surreal .

EXPOSE 8000 33693

ENTRYPOINT ["/surreal"]