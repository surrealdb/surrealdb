# Surrealism Runtime

Essential context for AI assistants working with the host runtime.

## Architecture

- **Engines:** `epoch.rs`. A pool of up to two `wasmtime::Engine`s (guarded and fast) per `EngineSettings`, shared process-wide by every holder of the same settings. Both use the pooling allocator with copy-on-write memory images. One ticker thread per guarded engine advances the epoch.
- **Runtime:** One compiled archive. Holds the `InstancePre` per engine flavour, the exports manifest and a `WasiTemplate` (the attached filesystem, opened once). No tenant or request state. Thread-safe, `Arc<Runtime>`.
- **Controller:** One fresh instance serving one invocation, dropped when it returns.

## Isolation Model

Every invocation runs in fresh memory:

1. **Slot** — `Engines::acquire_slot`. The slot count is
   `EngineSettings::max_concurrent_invocations`, the pooling allocator's size.
   A top-level invocation waits, bounded by its caller's deadline, and
   top-level invocations together hold at most all but
   `EngineSettings::nested_reserve` slots (a quarter, at least one). A nested
   one (`Call::nested`) takes any free slot, reserved or not, or fails at once
   with `NestedInvocationNoSlot`, because the slots may all be held by callers
   waiting on it.
2. **Instantiate** — from the `InstancePre` into the slot (a component that
   still exports `init` was refused when it was compiled), with the
   invocation's own `InstanceSettings`: network policy, memory limit, execution
   time limit, engine flavour, stdio.
3. **Invoke** — once per row. Each row is lent its own `Transaction` entry in
   the resource table, deleted on every path including a trap.
4. **Drop** — the store goes, and the slot returns with its memory reset to
   the module's image.

Nothing written survives the invocation: linear memory, globals, tables, the
resource table and stdio buffers are all per store. The rows of one invocation
share its memory.

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

Capabilities belong to the invocation, not the compiled component: two
definitions of one archive may grant different ones.

- `sql` / `run` permissions are checked by the `InvocationContext`, which holds
  the invocation's grant.
- The granted `net` targets gate networking twice, both from one `NetPolicy` (`net_allow.rs`)
  carried in `StoreData::net`: by **name** in `dns.rs`, which shadows
  `wasi:sockets/ip-name-lookup` and reads the policy from the store on each
  lookup, and by **address** in WASI's `socket_addr_check`. Each invocation gets
  a fresh copy (`NetPolicy::for_invocation`), so addresses vended to one are not
  recognised in another. Allowed names are not pre-resolved; loopback/private/link-local
  results are dropped unless an explicit IP rule covers them.
- An operator's hostname deny rule refuses the name at the DNS shim. When the
  grant also allows by address (`*`, a CIDR or an IP), `for_invocation`
  resolves each denied hostname once for the invocation and refuses those
  addresses too, so a guest cannot reach a denied host by its IP literal. If a
  denied name does not resolve within `DENY_RESOLVE_TIMEOUT`, the invocation
  may connect only to addresses the DNS shim vended.

## Epoch-Based Timeouts (Two-Engine Model)

Two wasmtime engines are maintained in `epoch.rs`:

- **Guarded engine**: `epoch_interruption(true)`. Compiles epoch checks into WASM at every loop back-edge and function call. Enables accurate timeout enforcement but adds ~10% overhead on typical code, up to ~2x on tight numerical loops (e.g. ML inference).
- **Fast engine**: No epoch interruption. Full native speed, no timeout enforcement.

Both use Cranelift, in every profile: Winch rejects
`wasm_component_model_async`, which `invoke` and the `transaction` methods
require.

The invocation's `InstanceSettings::strict_timeout` picks the engine (default `true` → guarded); a flavour is compiled on first use (`Runtime::prepare`). A definition asks for the fast engine with `CAPABILITIES { strict_timeout: false }`, and gets it only on a server started with `SURREAL_SURREALISM_ALLOW_UNBOUNDED_EXECUTION=true`. **Security:** the fast engine enforces no timeout at all; the module can run indefinitely and hold its slot and worker thread.

- Guarded: the ticker increments the epoch every `EPOCH_TICK_MS` (10ms) until the engine is dropped.
- Effective timeout = `min(context_remaining, module_limit, server_cap)`.
- Every guarded store's epoch deadline is one tick away; `epoch::yield_until_deadline` yields to the executor on each tick and traps with `Trap::Interrupt` once `StoreData::deadline` is reached, which maps to `SurrealismError::Timeout`. A guest computing without host calls so gives its worker thread back every tick.

## Key Files

| File | Purpose |
|------|---------|
| `runtime.rs` | `Runtime`, `Call`, `InstanceSettings`; `invoke` (slot, instantiate, init, rows, drop) |
| `controller.rs` | One fresh instance: invoke, init, list/args/returns |
| `store.rs` | `StoreData` (wasi, table, net policy, limiter) |
| `host.rs` | `InvocationContext`, `Transaction`, `implement_host_functions` |
| `epoch.rs` | `Engines` (pooled fast + guarded), `EngineSettings`, slots, epoch ticker |
| `capabilities.rs` | `SurrealismCapabilities`, `FunctionTargets` |
| `config.rs` | `SurrealismConfig`, `AbiVersion` |
| `package.rs` | `SurrealismPackage`, pack/unpack `.surli` |
| `exports.rs` | `ExportsManifest`, `FunctionExport` |
| `net_allow.rs` | `NetPolicy`, `NetRule` — one policy for both network checks |
| `dns.rs` | Name-filtering replacement for `wasi:sockets/ip-name-lookup` |
| `wasi_context.rs` | `WasiTemplate`: per-archive filesystem, per-invocation WASI context |
