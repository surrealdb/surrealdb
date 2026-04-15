# surrealism-types

Shared types for the Surrealism WASM plugin system in SurrealDB.

## Overview

`surrealism-types` provides the common types used by both the guest SDK (`surrealism`) and the host runtime (`surrealism-runtime`):

- **`Args` trait** (`args.rs`) — Typed argument marshalling between Rust tuples and `surrealdb_types::Value` vectors. Implemented for tuples of 0–10 elements and `Vec<T>`.
- **`SurrealismError`** (`err.rs`) — Unified error type covering compilation, instantiation, ABI mismatches, and runtime errors.
- **`PrefixErr` trait** (`err.rs`) — Extension trait for adding context to errors (similar to anyhow's `Context`).

## Usage

### Function Arguments

```rust
use surrealism_types::args::Args;

// Convert typed arguments to Values
let args = ("hello".to_string(), 42i64);
let values = args.to_values();

// Reconstruct typed arguments from Values
let restored: (String, i64) = Args::from_values(values).unwrap();
```

## Feature Flags

### `host`

Enable for host-side (runtime) code:

```toml
[dependencies]
surrealism-types = { version = "*", features = ["host"] }
```

When enabled, adds Wasmtime error variants to `SurrealismError`.

## Related Crates

- **surrealism-runtime** — Host-side WASM runtime implementation
- **surrealism** — Guest SDK for building WASM modules
- **surrealism-macros** — Procedural macros for the `#[surrealism]` attribute
- **surrealdb-types** — Core SurrealDB type system
