use uuid::Uuid;

use super::lookup::Lookup;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::val::TableName;

#[derive(Hash, Eq, PartialEq)]
pub(crate) enum Key {
	/// A cache key for a JWKS document (hashed URL or stable id)
	#[cfg(feature = "jwks")]
	Jwk(String),
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
	/// A cache key for live queries version (on a table)
	Lvv(NamespaceId, DatabaseId, TableName),
}

impl<'a> From<Lookup<'a>> for Key {
	fn from(value: Lookup<'a>) -> Self {
		match value {
			#[cfg(feature = "jwks")]
			Lookup::Jwk(a) => Key::Jwk(a.to_string()),
			Lookup::Fds(a, b, c, d) => Key::Fds(a, b, c.to_string(), d),
			Lookup::Evs(a, b, c, d) => Key::Evs(a, b, c.to_string(), d),
			Lookup::Fts(a, b, c, d) => Key::Fts(a, b, c.to_string(), d),
			Lookup::Ixs(a, b, c, d) => Key::Ixs(a, b, c.to_string(), d),
			Lookup::Lvs(a, b, c, d) => Key::Lvs(a, b, c.to_string(), d),
			Lookup::Lvv(a, b, c) => Key::Lvv(a, b, c.clone()),
		}
	}
}
