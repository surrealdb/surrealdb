# SurrealDB MCP server

This crate is the built-in [Model Context Protocol](https://modelcontextprotocol.io)
server for SurrealDB. It is shipped as part of `surreal start` (HTTP
transport at `/mcp`) and as the dedicated `surreal mcp` subcommand
(stdio transport).

The MCP surface is a thin adapter on top of the embedded `Datastore`:
identifiers are validated, data values are bound as typed `Variables`,
and per-call timeouts plus result caps keep the LLM context window
bounded. All authorization is delegated to the same access-control
machinery that backs `/sql` and `/rpc`, so capability rules and
`DEFINE USER ... PERMISSIONS ...` policies apply identically.

## Running the server

### HTTP transport (production / browser-driven clients)

`/mcp` is mounted automatically by `surreal start` when the `mcp`
feature is enabled. Every request flows through SurrealDB's `SurrealAuth`
middleware first, so the same `Authorization: Bearer <jwt>` /
`Authorization: Basic ...` headers used elsewhere apply.

```sh
surreal start --user root --pass root --bind 127.0.0.1:8000 memory
# MCP endpoint: http://127.0.0.1:8000/mcp
```

### Stdio transport (local IDE integrations)

```sh
surreal mcp --ns my_ns --db my_db memory
```

Stdio runs the MCP server in-process under `Session::owner()`. The
operator who launched the binary owns every tool call. **Do not expose
the stdio entry point to untrusted users** — it has no per-call auth
re-binding because there is no network surface to bind credentials
against.

## Security model

This server treats the `mcp-session-id` header as a **bearer token**.
Anyone with a live session id can replay tool calls under the original
caller's identity for the lifetime of that session. The hardening below
mitigates the spec-defined attack surface but cannot replace
deployment-level controls.

### Per-request subject verification

On `initialize`, the authenticated `Session` is captured into a
`BoundSubject` fingerprint (level + identity). Every subsequent tool
call, resource read, and prompt invocation re-extracts the request's
session and rejects the call with a JSON-RPC `invalid_params` error if a
**different** authenticated identity is presented. Requests without
credentials continue to run under the bound subject so existing clients
that send headers only on the handshake keep working.

If a session is initialised without credentials it is bound as anonymous
and cannot be upgraded by sending credentials on later requests; the
client must `DELETE /mcp` and re-initialise with credentials.
Authenticated initialisation is the only point at which the subject is
captured.

### Audit log

Every tool invocation emits exactly one structured `tracing::info!`
record on the `surrealdb::mcp::audit` target with these fields:

| Field | Description |
|---|---|
| `tool` | The MCP tool name (`query`, `select`, `run`, …). |
| `subject` | `<level>::<id>` for authenticated callers, `anonymous` otherwise. |
| `namespace` / `database` | The session's `use` context at call time. |
| `outcome` | `ok`, `tool_error`, or `protocol_error`. |
| `kind` | Best-effort error kind (`Validation`, `NotFound`, …). |
| `time_ms` | Wall-clock duration of the handler. |

No query text, parameter values, or row payloads are ever logged —
forward the audit target to your SIEM as-is.

### Operator hardening checklist

- **Run behind TLS.** The `mcp-session-id` header is sensitive; never
  expose it over plaintext HTTP.
- **Lock CORS.** Set `--allow-origin` to the explicit origins that
  should be allowed; the default `*` is fine for development but
  inappropriate for production browser clients.
- **Bind narrowly.** `surreal start` defaults to `127.0.0.1:8000`. Keep
  it that way unless your network already enforces ingress controls.
- **Use SurrealDB capabilities** (`--allow-funcs`, `--deny-funcs`,
  `--allow-net`, …) to restrict what SurrealQL can do. The MCP tools
  inherit capability decisions; locking down `http::*`, `sql::*`, and
  filesystem-touching functions limits blast radius if a session is
  hijacked.
- **Define a least-privilege user** for MCP clients via
  `DEFINE USER ... ON DATABASE ROLES VIEWER` (or a custom role with
  table-level `PERMISSIONS`) rather than handing out root credentials.
- **Idle timeout.** The streamable-HTTP layer expires sessions after 5
  minutes of inactivity; do not extend this without a reason.

## Configuration

All MCP-specific knobs are read once on first use via `LazyLock` from
their `SURREAL_MCP_*` environment variables. HTTP body limits live with
the rest of the server's body caps.

| Variable | Default | Effect |
|---|---|---|
| `SURREAL_MCP_QUERY_TIMEOUT_SECS` | 60 | Outer timeout on every `execute` call. `0` disables. |
| `SURREAL_MCP_MAX_RESULT_BYTES` | 256 KiB | Per-call cap on serialized tool / resource bodies; over-cap responses are replaced with a truncation marker. `0` disables. |
| `SURREAL_MCP_RUN_MAX_ARGS` | 64 | Maximum arguments for a single `run` invocation. |
| `SURREAL_MCP_PARAMS_MAX_KEYS` | 256 | Maximum top-level keys in a `parameters` / `*_data` JSON object. |
| `SURREAL_HTTP_MAX_MCP_BODY_SIZE` | 4 MiB | Maximum HTTP request body for `/mcp`. |

JSON nesting depth is not capped at the MCP layer because `serde_json`
already enforces a hard 128-level recursion limit during deserialization.

## Test layout

- `surrealdb/mcp/src/**/*.rs` — unit tests live alongside the modules
  they cover.
- `surrealdb/mcp/tests/basic.rs` — direct in-process tool tests.
- `surrealdb/mcp/tests/stdio.rs` — full MCP handshake over an in-memory
  duplex pipe (mirrors the real stdio transport).
- `surrealdb/mcp/tests/http.rs` — `StreamableHttpService` driven via
  hand-built `http::Request`s, matching production exactly.
