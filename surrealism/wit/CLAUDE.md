# Surrealism WIT

Essential context for AI assistants working with the WIT protocol.

## Overview

The `surrealism:extension` package defines the contract between SurrealDB (host) and guest WASM modules. All crossing-boundary data is FlatBuffers-serialized; WIT uses `list<u8>` aliases for those buffers.

## Host Interface

Imported by the guest (`surrealism:extension/host`). Nothing is ambient: both
functions are methods on a `transaction` resource the host lends per call.

```wit
resource transaction {
    sql: async func(query, vars) -> result<serialized-value, host-error>;
    run: async func(fnc, version?, args) -> result<serialized-value, host-error>;
}
```

`async func` means each call becomes a component-model subtask, so a guest that
awaits two reads together genuinely overlaps them. The host serialises the ones
that may write against everything else — see the async model section in
`surrealism/CLAUDE.md`.

Both return `result<T, host-error>`, a variant whose case says what kind of
failure occurred: `permission-denied`, `invalid-request`, `datastore`,
`deadline-exceeded` or `other`. A denied call never reaches the datastore, so
this is an observability and ergonomics boundary rather than a security one.

The host owns every handle and deletes it from the store's resource table when
`invoke` returns, so a retained handle traps rather than working. An export lent
no handle — `init` and the introspection exports — has no route to the host.

## Guest Exports

Required / expected from the guest:

- **invoke** — `async`; `(tx?, name?, args)` → calls the named function (or default if `name` is `none`)
- **list-functions** — returns `list<option<string>>`
- **function-args** — `(name?)` → serialized kind list
- **function-returns** — `(name?)` → serialized kind
- **function-writeable** — `(name?)` → whether the function may write
- **function-comment** — `(name?)` → optional comment
- **init** — optional; called once after instantiation. Sync, and lent no `tx`.

Only `invoke` is async-lifted. The introspection exports never call a host
import, and a sync-lifted export cannot block, so they stay synchronous.

## FlatBuffers Serialization

All `serialized-*` types are FlatBuffer byte buffers. Schemas live in `surrealdb/types/src/flatbuffers/` and must stay in sync between host and guest.

- `serialized-value` — SurrealDB value
- `serialized-vars` — bind variables (map)
- `serialized-args` — ordered function arguments
- `serialized-kind` / `serialized-kinds` — type descriptors

## Lifecycle

1. Host instantiates the WASM component.
2. Host calls **init** if exported. On error, module is marked unhealthy.
3. Host reads the function's entry in the baked `exports.toml`, installs a
   no-write execution frame unless the entry is `writeable`, lends a
   `transaction`, and calls **invoke** with `(tx?, name?, args)`. The handle is
   revoked on return.
4. Introspection (`list-functions`, `function-args`, `function-returns`,
   `function-writeable`, `function-comment`) is read once by
   `surreal module build`, which bakes the answers into `exports.toml`. The host
   reads that manifest at runtime and does not call these exports.

## ABI Stability

Protocol changes should be minimized. WIT changes can break compatibility. ABI
version in `package.toml` tracks compatibility; current is 3. It is bumped at
most once per release that changes this world — see the ABI section in
`surrealism/CLAUDE.md` for what that gate catches and what it does not.
