# Surrealism WIT

Essential context for AI assistants working with the WIT protocol.

## Overview

The `surrealism:plugin` package defines the contract between SurrealDB (host) and guest WASM modules. All crossing-boundary data is FlatBuffers-serialized; WIT uses `list<u8>` aliases for those buffers.

## Host Interface

Imported by the guest (`surrealism:plugin/host`):

- **sql** — `(query, vars)` → execute SurrealQL, returns serialized value
- **run** — `(fnc, version?, args)` → call another surrealism function by FQN
- **kv-get**, **kv-set**, **kv-del**, **kv-exists** — single-key ops
- **kv-del-rng**, **kv-get-batch**, **kv-set-batch**, **kv-del-batch** — batch/range ops
- **kv-keys**, **kv-values**, **kv-entries**, **kv-count** — range queries

All return `result<T, string>`; errors are human-readable strings.

## Guest Exports

Required / expected from the guest:

- **invoke** — `(name?, args)` → calls the named function (or default if `name` is `none`)
- **list-functions** — returns `list<option<string>>`
- **function-args** — `(name?)` → serialized kind list
- **function-returns** — `(name?)` → serialized kind
- **init** — optional; called once after instantiation

## FlatBuffers Serialization

All `serialized-*` types are FlatBuffer byte buffers. Schemas live in `surrealdb/types/src/flatbuffers/` and must stay in sync between host and guest.

- `serialized-value` — SurrealDB value
- `serialized-vars` — bind variables (map)
- `serialized-args` — ordered function arguments
- `serialized-kind` / `serialized-kinds` — type descriptors

## Lifecycle

1. Host instantiates the WASM component.
2. Host calls **init** if exported. On error, module is marked unhealthy.
3. Host calls **invoke** per request with `(name?, args)`.
4. Introspection (`list-functions`, `function-args`, `function-returns`) is used at build time and by `surreal module info`.

## ABI Stability

Protocol changes should be minimized. WIT changes can break compatibility. ABI version in `surrealism.toml` tracks compatibility; current is 2.
