# Telemetry

SurrealDB leverages the tracing and opentelemetry libraries to instrument the code.

Both metrics and traces are pushed to an OTEL compatible receiver.

For local development, you can start the observability stack defined in `dev/docker`. It spins up an instance of Opentelemetry collector, Grafana, Prometheus and Tempo:

```
$ docker-compose -f dev/docker/compose.yaml up -d
$ SURREAL_TELEMETRY_PROVIDER=otlp OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317" surreal start
```

Now you can use the SurrealDB server and see the telemetry data opening this URL in the browser: http://localhost:3000

To login into Grafana, use the default user `admin` and password `admin`.
