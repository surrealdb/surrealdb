# surrealdb-datastore

The SurrealDB datastore layer: the keyspace, and the durable shapes the values
behind those keys take. How a key encodes is one layer down, in `surrealdb-kvs`;
what the engine does with a decoded value is one layer up.

**This crate is an internal implementation detail of SurrealDB.** Its API is
unstable and changes without notice; depend on the `surrealdb` SDK or
`surrealdb-core` instead.
