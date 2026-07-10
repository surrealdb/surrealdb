# surrealdb-kvs

Common types and traits shared by every SurrealDB key-value store backend: the
`Transactable` transaction trait, the `TransactionBuilder` datastore
abstraction, the raw `Key`/`KeyRange`/`Val` byte types, and the shared error,
configuration, cursor, and timestamp machinery.

> [!WARNING]
> This crate is SurrealDB internal API. It does not adhere to SemVer and its
> API is free to change and break code even between patch versions. If you are
> looking for a stable interface to the SurrealDB library please have a look at
> the [Rust SDK](https://crates.io/crates/surrealdb).
