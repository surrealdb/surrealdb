# Surrealism Runtime

Essential context for AI assistants working with the host runtime.

## Architecture

- **Engine:** Shared global `wasmtime::Engine` via `epoch.rs`. One background thread ticks the epoch for cooperative preemption.
- **Runtime:** Per-module. Compiles WASM once, owns controller pool and config. Thread-safe, `Arc<Runtime>`.
- **Controller:** Per-invocation. Single-threaded. Holds Store + Instance; can be reused by swapping host context.

## Pooling Model

Controllers are pooled and reused. Flow:

1. **Acquire** — pop from pool or create new, run init() if new. Points the WASI
   stdio callbacks at this request's logging path.
2. **Invoke** — push a `Transaction` into the store's resource table, call
   `invoke(tx?, name, args)`, then delete the entry on every path including a
   trap.
3. **Release** — reset stdio to host defaults and return to pool. Do not release
   after a WASM trap; drop the controller.

Controllers hold no request state: datastore access is granted only by the
handle lent for one call. WASM linear memory and statics persist across calls.

## Controller Lifecycle

`acquire_controller(&context)` → `invoke` / `invoke_with_timeout(Some(context), …)` → `release_controller`

On trap: drop controller; do not release to pool. After a trap the instance
reports `Trap::CannotEnterComponent` on the next call, so reusing it is not an
option anyway.

## Host Function Contract

`InvocationContext` trait provides:

- `sql(query, vars)` — run SurrealQL
- `run(fnc, version, args)` — call another surrealism function by FQN
- `stdout_callback` / `stderr_callback` — WASI stdio logging

Methods take `&self` and the trait is shared as `Arc<dyn InvocationContext>`,
because a guest may hold several host calls in flight and the store is handed
back to the guest while any is pending.

`host.rs` registers `sql`/`run` as methods on the `transaction` resource with
`func_wrap_concurrent`. Each resolves the handle into its context through the
store's resource table **before** the first await — `Accessor::with` is
synchronous and cannot span one.

That resolution also takes a place among the calls in flight through the handle,
capped at `MAX_IN_FLIGHT_HOST_CALLS`, and releases it on every path out. The cap
bounds the fan-out; it does not order it. Whether two calls may run at the same
time is a question about the statements they carry, so the `InvocationContext`
decides it — see `Host::order` in `surrealdb/core/src/surrealism/host.rs`.

## Capability Enforcement

- `allow_functions`: `FunctionTargets` enum — `None` (deny all), `All`, or `Some(patterns)`.
- `run()` checks `config.capabilities.allow_functions.allows(fnc)` before executing.
- `allow_arbitrary_queries` for `sql()`.
- `allow_net` gates networking twice, both from one `NetPolicy` (`net_allow.rs`):
  by **name** in `dns.rs`, which shadows `wasi:sockets/ip-name-lookup`, and by
  **address** in WASI's `socket_addr_check`. Nothing is pre-resolved; the shim
  records the addresses it vends so the connect recognises them, and drops
  loopback/private/link-local results unless an explicit IP rule covers them.

## Epoch-Based Timeouts (Two-Engine Model)

Two wasmtime engines are maintained in `epoch.rs`:

- **Guarded engine**: `epoch_interruption(true)`. Compiles epoch checks into WASM at every loop back-edge and function call. Enables accurate timeout enforcement but adds ~10% overhead on typical code, up to ~2x on tight numerical loops (e.g. ML inference).
- **Fast engine**: No epoch interruption. Full native speed, no timeout enforcement.

Both use Cranelift, in every profile: Winch rejects
`wasm_component_model_async`, which `invoke` and the `transaction` methods
require.

Modules choose via `strict_timeout` in `surrealism.toml` (default `true` → guarded). Compute-heavy trusted modules set `strict_timeout = false` for the fast engine. **Security:** `strict_timeout = false` means no timeout enforcement at all; the module can run indefinitely. Only explicitly trusted modules should use this. Code signing (planned) will gate access.

- Guarded: shared global ticker increments epoch every `EPOCH_TICK_MS` (10ms).
- Effective timeout = `min(context_remaining, module_limit, server_cap)`.
- `store.set_epoch_deadline(ticks)`; trap `Trap::Interrupt` maps to `SurrealismError::Timeout`.

## Key Files

| File | Purpose |
|------|---------|
| `runtime.rs` | Runtime, pool, `acquire_controller` / `release_controller`, `new_controller` |
| `controller.rs` | Per-execution controller, invoke, init, list/args/returns |
| `store.rs` | `StoreData` (wasi, table, config, limiter, stdio callbacks) |
| `host.rs` | `InvocationContext`, `Transaction`, `implement_host_functions` |
| `epoch.rs` | Two shared engines (fast + guarded), `shared_engine(guarded)`, epoch ticker |
| `capabilities.rs` | `SurrealismCapabilities`, `FunctionTargets` |
| `config.rs` | `SurrealismConfig`, `AbiVersion` |
| `package.rs` | `SurrealismPackage`, pack/unpack `.surli` |
| `exports.rs` | `ExportsManifest`, `FunctionExport` |
| `net_allow.rs` | `NetPolicy`, `NetRule` — one policy for both network checks |
| `dns.rs` | Name-filtering replacement for `wasi:sockets/ip-name-lookup` |
| `wasi_context.rs` | Build WASI ctx for preopened dirs, networking |
