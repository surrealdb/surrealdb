use surrealdb_catalog::{DatabaseId, NamespaceId};
use uuid::Uuid;

use super::lookup::Lookup;

#[derive(Hash, Eq, PartialEq)]
pub enum Key {
	/// A cache key for a JWKS document (hashed URL or stable id)
	#[cfg(feature = "jwks")]
	Jwk(String),
	/// A cache key for one table's catalog entry under one catalog version
	Catalog(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for the catalog entry of a table no catalog write has
	/// stamped, under the content hash of its definition
	CatalogUnstamped(NamespaceId, DatabaseId, String, u64),
	/// A cache key for one function's compiled definition under one content hash
	Function(NamespaceId, DatabaseId, String, u64),
	/// A cache key for fields (on a table)
	Fds(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for events (on a table)
	Evs(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for views (on a table)
	Fts(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for indexes (on a table)
	Ixs(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for live queries (on a table)
	Lvs(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for the value shapes a table's online B-tree indexes have
	/// stored, under one catalog version
	Ish(NamespaceId, DatabaseId, String, Uuid),
}

impl<'a> From<Lookup<'a>> for Key {
	fn from(value: Lookup<'a>) -> Self {
		match value {
			#[cfg(feature = "jwks")]
			Lookup::Jwk(a) => Key::Jwk(a.to_string()),
			Lookup::Catalog(a, b, c, d) => Key::Catalog(a, b, c.to_string(), d),
			Lookup::CatalogUnstamped(a, b, c, d) => Key::CatalogUnstamped(a, b, c.to_string(), d),
			Lookup::Function(a, b, c, d) => Key::Function(a, b, c.to_string(), d),
			Lookup::Fds(a, b, c, d) => Key::Fds(a, b, c.to_string(), d),
			Lookup::Evs(a, b, c, d) => Key::Evs(a, b, c.to_string(), d),
			Lookup::Fts(a, b, c, d) => Key::Fts(a, b, c.to_string(), d),
			Lookup::Ixs(a, b, c, d) => Key::Ixs(a, b, c.to_string(), d),
			Lookup::Lvs(a, b, c, d) => Key::Lvs(a, b, c.to_string(), d),
			Lookup::Ish(a, b, c, d) => Key::Ish(a, b, c.to_string(), d),
		}
	}
}
