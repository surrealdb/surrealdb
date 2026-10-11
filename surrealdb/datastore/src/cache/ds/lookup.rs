use quick_cache::Equivalent;
use surrealdb_catalog::{DatabaseId, NamespaceId};
use uuid::Uuid;

use super::key::Key;

#[derive(Hash, Eq, PartialEq)]
pub enum Lookup<'a> {
	/// A cache key for a JWKS document
	#[cfg(feature = "jwks")]
	Jwk(&'a str),
	/// A cache key for one table's catalog entry under one catalog version
	Catalog(NamespaceId, DatabaseId, &'a str, Uuid),
	/// A cache key for the catalog entry of a table no catalog write has
	/// stamped, under the content hash of its definition
	CatalogUnstamped(NamespaceId, DatabaseId, &'a str, u64),
	/// A cache key for one function's compiled definition under one content hash
	Function(NamespaceId, DatabaseId, &'a str, u64),
	/// A cache key for fields (on a table)
	Fds(NamespaceId, DatabaseId, &'a str, Uuid),
	/// A cache key for events (on a table)
	Evs(NamespaceId, DatabaseId, &'a str, Uuid),
	/// A cache key for views (on a table)
	Fts(NamespaceId, DatabaseId, &'a str, Uuid),
	/// A cache key for indexes (on a table)
	Ixs(NamespaceId, DatabaseId, &'a str, Uuid),
	/// A cache key for live queries (on a table)
	Lvs(NamespaceId, DatabaseId, &'a str, Uuid),
	/// A cache key for the value shapes a table's online B-tree indexes have
	/// stored, under one catalog version
	Ish(NamespaceId, DatabaseId, &'a str, Uuid),
}

impl Equivalent<Key> for Lookup<'_> {
	#[rustfmt::skip]
	fn equivalent(&self, key: &Key) -> bool {
		match (self, key) {
			#[cfg(feature = "jwks")]
			(Self::Jwk(la), Key::Jwk(ka)) => la == ka,
			(Self::Catalog(la, lb, lc, ld), Key::Catalog(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::CatalogUnstamped(la, lb, lc, ld), Key::CatalogUnstamped(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Function(la, lb, lc, ld), Key::Function(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Fds(la, lb, lc, ld), Key::Fds(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Evs(la, lb, lc, ld), Key::Evs(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Fts(la, lb, lc, ld), Key::Fts(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Ixs(la, lb, lc, ld), Key::Ixs(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Lvs(la, lb, lc, ld), Key::Lvs(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Ish(la, lb, lc, ld), Key::Ish(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			_ => false,
		}
	}
}
