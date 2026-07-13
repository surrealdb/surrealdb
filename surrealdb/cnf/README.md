# SurrealDB Config

SurrealDB's configuration system: the [`Config`] trait and [`ConfigMap`]
key-value source used by every configurable component (core, the KV storage
backends, the server, and enterprise), plus the server-wide configuration
constants, environment-variable defaults, and the [`CommonConfig`] struct used
to tune SurrealDB's runtime limits (recursion depths, cache sizes, batch
sizes, GQL resource guards, and more).

> **Note.** This crate is part of SurrealDB's **internal API**. It is
> published to crates.io because the workspace requires it for
> `surrealdb-core`, but it offers **no stability guarantees** between
> releases. For a stable interface to SurrealDB, use the
> [Rust SDK](https://crates.io/crates/surrealdb).

[`Config`]: https://docs.rs/surrealdb-cnf/latest/surrealdb_cnf/trait.Config.html
[`ConfigMap`]: https://docs.rs/surrealdb-cnf/latest/surrealdb_cnf/struct.ConfigMap.html
[`CommonConfig`]: https://docs.rs/surrealdb-cnf/latest/surrealdb_cnf/struct.CommonConfig.html
