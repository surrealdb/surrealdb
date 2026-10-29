# surrealdb-kvs-any

Facade over every SurrealDB key-value store backend: constructs the right
datastore from a connection path string (`memory`, `surrealkv://path`),
with each backend gated behind a matching `kv-*` cargo feature. Other backends,
such as the TiKV backend in
[surrealdb-tikv](https://github.com/surrealdb/surrealdb-tikv), plug in by
registering a `BackendProvider`.

> [!WARNING]
> This crate is SurrealDB internal API. It does not adhere to SemVer and its
> API is free to change and break code even between patch versions. If you are
> looking for a stable interface to the SurrealDB library please have a look at
> the [Rust SDK](https://crates.io/crates/surrealdb).
