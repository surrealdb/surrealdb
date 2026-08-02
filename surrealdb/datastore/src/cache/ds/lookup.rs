use quick_cache::Equivalent;
use surrealdb_catalog::{DatabaseId, NamespaceId};
use uuid::Uuid;

use super::key::Key;

#[derive(Hash, Eq, PartialEq)]
pub enum Lookup<'a> {
	/// A cache key for a JWKS document
	#[cfg(feature = "jwks")]
	Jwk(&'a str),
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
}

impl Equivalent<Key> for Lookup<'_> {
	#[rustfmt::skip]
	fn equivalent(&self, key: &Key) -> bool {
		match (self, key) {
			#[cfg(feature = "jwks")]
			(Self::Jwk(la), Key::Jwk(ka)) => la == ka,
			(Self::Fds(la, lb, lc, ld), Key::Fds(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Evs(la, lb, lc, ld), Key::Evs(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Fts(la, lb, lc, ld), Key::Fts(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Ixs(la, lb, lc, ld), Key::Ixs(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Lvs(la, lb, lc, ld), Key::Lvs(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			_ => false,
		}
	}
}
