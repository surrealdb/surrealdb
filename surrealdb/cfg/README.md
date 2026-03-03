# SurrealDB Config

Typed configuration structs for the SurrealDB core database engine.
This crate should not be used outside of SurrealDB itself.
For a stable interface to the SurrealDB library see [the Rust SDK](https://crates.io/crates/surrealdb)

`surrealdb-cfg` provides a single top-level `CoreConfig` that groups every tunable parameter the
core engine accepts. Each section is its own struct so it can be passed independently to the
subsystem that needs it.

## Configuration Sections

| Struct | Purpose |
|---|---|
| **`LimitsConfig`** | Computation depth, parsing depth, concurrency, regex size, and sorting thresholds |
| **`ScriptingConfig`** | JavaScript function runtime limits (stack size, memory, execution time) |
| **`HttpClientConfig`** | Outgoing HTTP client settings (redirects, connection pools, timeouts, user-agent) |
| **`CacheConfig`** | Transaction, datastore, surrealism module, and HNSW vector cache sizes |
| **`BatchConfig`** | Key-scan batch sizes for general, export, count, and indexing queries |
| **`SecurityConfig`** | Security flags such as forwarding access errors to clients |
| **`FileConfig`** | File and bucket folder access allowlists and global bucket settings |

## Features

- **`cli`** — Derives `clap::Args` on every config struct so the server binary can parse them
  directly from command-line flags and environment variables (e.g. `SURREAL_MAX_COMPUTATION_DEPTH`).
  Disabled by default; only the server crate enables it.

## Usage

Without the `cli` feature every struct implements `Default`, so embedding the config requires no
extra dependencies:

```rust
use surrealdb_cfg::CoreConfig;

let config = CoreConfig::default();
```

## License

This crate is part of SurrealDB and follows the same licensing terms.
