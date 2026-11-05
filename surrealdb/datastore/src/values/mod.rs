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

/// Implements [`KVValue`](surrealdb_kvs::value::KVValue) for a `revision` container that holds a
/// catalog entry through [`EmbeddedEntry`](surrealdb_catalog::stored::EmbeddedEntry).
///
/// The codec is the `revision` one. A read that fails because the held entry needs a newer release
/// fails with that refusal as a catalog [`Error::Entry`](surrealdb_catalog::Error::Entry), not
/// with the container's own error, so the reader is told which release it needs.
macro_rules! impl_kv_value_embedding {
	($name:ident) => {
		impl surrealdb_kvs::value::KVValue for $name {
			type KeyContext = ();

			#[inline]
			fn kv_encode_value(&self) -> ::anyhow::Result<Vec<u8>> {
				Ok(::revision::to_vec(self)?)
			}

			#[inline]
			fn kv_decode_value(bytes: &[u8], _: ()) -> ::anyhow::Result<Self> {
				::revision::from_slice(bytes).map_err(|error| {
					let error = ::anyhow::Error::from(error);
					match error.chain().find_map(surrealdb_catalog::stored::EntryError::of_cause) {
						Some(entry) => surrealdb_catalog::Error::Entry(entry).into(),
						None => error,
					}
				})
			}
		}
	};
}

pub mod catalog_version;
pub mod changefeed;
pub mod diskann;
pub mod entry;
pub mod event_queue;
pub mod fulltext;
pub mod graph;
pub mod hnsw;
pub mod ids;
pub mod index_build;
pub mod index_delta;
pub mod inline_cache;
pub mod live_query;
pub(crate) mod save_point;
pub mod session;
pub mod vector;
