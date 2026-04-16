# Surrealism Runtime

Essential context for AI assistants working with the host runtime.

## Architecture

- **Engine:** Shared global `wasmtime::Engine` via `epoch.rs`. One background thread ticks the epoch for cooperative preemption.
- **Runtime:** Per-module. Compiles WASM once, owns controller pool, KV store, config. Thread-safe, `Arc<Runtime>`.
- **Controller:** Per-invocation. Single-threaded. Holds Store + Instance; can be reused by swapping host context.

## Pooling Model

Controllers are pooled and reused. Flow:

1. **Acquire** — pop from pool or create new, run init() if new.
2. **Set context** — install `InvocationContext` (auth, KV, sql/run handlers).
3. **Invoke** — call `invoke(name, args)`.
4. **Clear context** — replace with `NullContext` (no per-request state retained).
5. **Release** — return to pool. Do not release after a WASM trap; drop the controller.

When idle, pooled controllers have `NullContext`; WASM linear memory and statics persist across calls.

## Controller Lifecycle

`acquire_controller(context)` → `set_context` (if reused) → `invoke` / `invoke_with_timeout` → `clear_context` → `release_controller`

On trap: drop controller; do not release to pool.

## Host Function Contract

`InvocationContext` trait provides:

- `sql(query, vars)` — run SurrealQL
- `run(fnc, version, args)` — call another surrealism function by FQN
- `kv()` — per-module KV store
- `stdout` / `stderr` — logging output

Host functions in `host.rs` read `StoreData.context` and delegate to the current `InvocationContext`.

## Capability Enforcement

- `allow_functions`: `FunctionTargets` enum — `None` (deny all), `All`, or `Some(patterns)`.
- `run()` checks `config.capabilities.allow_functions.allows(fnc)` before executing.
- `allow_arbitrary_queries` for `sql()`; `allow_net` for WASI networking.

## KV Semantics

- Per-module `Arc<BTreeMapStore>` shared across all invocations.
- **Volatile:** Data lives only in process memory. Lost on server restart, module eviction from cache, or `Runtime` drop. Not suitable for durable state.
- Limits: `max_kv_entries`, `max_kv_value_bytes` (from config and server), `MAX_KV_KEY_BYTES` (1024 bytes, hardcoded).
- Keys are UTF-8 strings (max 1024 bytes); values are FlatBuffers-serialized SurrealDB values.
- Uses `parking_lot::RwLock` for interior mutability (non-poisoning).

## Epoch-Based Timeouts (Two-Engine Model)

Two wasmtime engines are maintained in `epoch.rs`:

- **Guarded engine**: `epoch_interruption(true)`. Compiles epoch checks into WASM at every loop back-edge and function call. Enables accurate timeout enforcement but adds ~10% overhead on typical code, up to ~2x on tight numerical loops (e.g. ML inference).
- **Fast engine**: No epoch interruption. Full native speed, no timeout enforcement.

Modules choose via `strict_timeout` in `surrealism.toml` (default `true` → guarded). Compute-heavy trusted modules set `strict_timeout = false` for the fast engine. **Security:** `strict_timeout = false` means no timeout enforcement at all; the module can run indefinitely. Only explicitly trusted modules should use this. Code signing (planned) will gate access.

- Guarded: shared global ticker increments epoch every `EPOCH_TICK_MS` (10ms).
- Effective timeout = `min(context_remaining, module_limit, server_cap)`.
- `store.set_epoch_deadline(ticks)`; trap `Trap::Interrupt` maps to `SurrealismError::Timeout`.

## Key Files

| File | Purpose |
|------|---------|
| `runtime.rs` | Runtime, pool, `acquire_controller` / `release_controller`, `new_controller` |
| `controller.rs` | Per-execution controller, invoke, init, list/args/returns |
| `store.rs` | `StoreData` (wasi, table, config, context, limiter) |
| `host.rs` | `InvocationContext`, `NullContext`, `implement_host_functions` |
| `epoch.rs` | Two shared engines (fast + guarded), `shared_engine(guarded)`, epoch ticker |
| `kv.rs` | `BTreeMapStore`, `KVStore` trait |
| `capabilities.rs` | `SurrealismCapabilities`, `FunctionTargets` |
| `config.rs` | `SurrealismConfig`, `AbiVersion` |
| `package.rs` | `SurrealismPackage`, pack/unpack `.surli` |
| `exports.rs` | `ExportsManifest`, `FunctionExport` |
| `wasi_context.rs` | Build WASI ctx for preopened dirs, networking |
