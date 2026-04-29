# Surrealism Types

Essential context for AI assistants working with the shared types crate.

## Purpose

`surrealism-types` provides types used by both the guest SDK (`surrealism`) and the host runtime (`surrealism-runtime`). It has no dependency on `wasmtime` unless the `host` feature is enabled.

## Key Types

### `Args` trait (`args.rs`)

Marshals typed function arguments to/from `Vec<surrealdb_types::Value>`:

- `to_values()` — consume typed tuple, produce value vector
- `from_values()` — reconstruct typed tuple from value vector (validates count and types)
- `kinds()` — return expected `Kind` per argument position

Implemented via `impl_args!` macro for tuples of 1–10 elements, `()` for zero args, and `Vec<T>` for variadic args. All element types must implement `surrealdb_types::SurrealValue`.

### `SurrealismError` enum (`err.rs`)

Domain error type with variants:

| Variant | Feature | When |
|---------|---------|------|
| `Compilation` | `host` | WASM compile failure |
| `Instantiation` | `host` | WASM instantiation failure |
| `FunctionCallError` | — | Guest function returned an error |
| `Timeout` | — | Epoch interrupt; carries effective/context/module timeouts |
| `UnsupportedAbi` | — | ABI version mismatch |
| `IntConversion` | — | `TryFromIntError` |
| `Wasmtime` | `host` | Catch-all wasmtime error |
| `Other` | — | Wrapped `anyhow::Error` |

### `PrefixErr` trait (`err.rs`)

Convenience for wrapping errors with contextual prefixes: `result.prefix_err(|| "loading config")?`.

## Feature Flags

- **`host`** — enables `wasmtime` dependency and host-only error variants (`Compilation`, `Instantiation`, `Wasmtime`). The runtime crate enables this; the guest SDK does not.

## Key Files

| File | Purpose |
|------|---------|
| `args.rs` | `Args` trait and tuple/vec implementations |
| `err.rs` | `SurrealismError`, `SurrealismResult`, `PrefixErr` |
