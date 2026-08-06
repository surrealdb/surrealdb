//! The values the keyspace binds.
//!
//! A key names the type its value decodes to, so every one of those types has to
//! be reachable from here. Most already are — a catalog definition, a record, a
//! sequence's state. The types in this module are the ones that would not be:
//! their *behaviour* belongs to a layer above, but the bytes they persist are
//! part of the keyspace and cannot live above it.
//!
//! So each carries only what persistence needs — the fields, their revision and
//! serde derives, and the [`KVValue`](surrealdb_kvs::value::KVValue) codec that
//! turns them into value bytes. The engine code that reads and mutates them —
//! building an index, walking a proximity graph, analysing text — stays where it
//! is and names these downward.
//!
//! Separating a type from the code that maintains it costs its fields their
//! privacy, since the invariant no longer lives in the same module as the data.
//! Where an invariant is worth more than the separation, the method that upholds
//! it comes here too, and is documented as belonging to the value rather than to
//! its caller.

pub mod changefeed;
pub mod diskann;
pub mod entry;
pub mod event_queue;
pub mod fulltext;
pub mod hnsw;
pub mod ids;
pub mod index_build;
pub mod index_delta;
pub mod live_query;
pub mod session;
pub mod vector;
