//! The engine's keyspace: which keys exist, and what each one spells.
//!
//! The layout is declared once, in the [`schema`] module, and every key type in
//! the keyspace is generated from that declaration. What the generated code is
//! written against — `KVKey`, `KVKeyDecode`, `KVSubspace`, `KVRange` and the
//! `TypedRange`/`RawRange` wrappers — is the storage layer's contract and lives
//! in [`surrealdb_kvs::key`]; this module re-exports it so a key type and the
//! traits it implements are named from one place.
//!
//! Generated names follow the layout: `XxKey` addresses one key, `XxRoot` is the
//! root of a level and prefixes everything beneath it, and `XxPrefix` is a bound
//! truncating a key at a declared point. Only the first stores a value; the
//! other two exist to be scanned, and produce a `TypedRange` when every key they
//! cover holds the same type and a `RawRange` when they span a whole region.
//!
//! Sigils, in the order a key spells them:
//!
//! - `/` the root of the keyspace, and `!` a catalog definition under it
//! - `*` a step down a level: namespace, database, table, then record
//! - `$` per-node state, `&` access grants at the root and references on a table
//! - `#` change feed, `%` live events, `~` graph edges
//! - `+` an index, whose entries are the only keys that encode under `storekey`'s `IndexFormat`
//!
//! The map below is generated from the same schema as the encoders and checked in
//! as `schema::KEYSPACE_MAP`, so it cannot drift from the code: a test fails if
//! the two disagree. Regenerate it with
//! `RESULT=OVERWRITE cargo test -p surrealdb-core keyspace_map`.
//!
//! The layout itself, and the map generated from it, live with the keyspace
//! in [`surrealdb_datastore::key`].

// The keyspace lives with the crate that owns the transaction; these bind it to
// core's own `crate::key::…` paths.
pub(crate) use surrealdb_datastore::key::{reclaim, schema};
pub(crate) use surrealdb_kvs::key::{
	AnyRange, Error, KVKey, KVKeyDecode, KVRange, KVSubspace, RawRange, Resumable, TypedRange,
};
pub(crate) use surrealdb_kvs::value::KVValue;
pub(crate) use surrealdb_kvs::{Key, KeyRange};
