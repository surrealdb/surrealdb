//! The engine's keyspace: which keys exist, and what each one spells.
//!
//! The layout is declared once, in [`schema`], and every key type is generated
//! from that declaration. What the generated code is written against - `KVKey`,
//! `KVRange` and the range wrappers - is the storage layer's contract and lives
//! in [`surrealdb_kvs::key`].
//!
//! Generated names follow the layout: `XxKey` addresses one key, `XxRoot` is the
//! root of a level and prefixes everything beneath it, and `XxPrefix` is a bound
//! truncating a key at a declared point. Only the first stores a value; the other
//! two exist to be scanned.
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
//! `RESULT=OVERWRITE cargo test -p surrealdb-datastore keyspace_map`.
//!
//! ```text
#![doc = include_str!("keyspace.map")]
//! ```
//!

pub mod reclaim;
pub mod schema;
