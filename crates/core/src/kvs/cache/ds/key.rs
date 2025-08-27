use uuid::Uuid;

use super::lookup::Lookup;
use crate::catalog::{DatabaseId, NamespaceId};

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) enum Key {
	/// A cache key for a database
	Db(String, String),
	/// A cache key for a table
	Tb(NamespaceId, DatabaseId, String),
	/// A cache key for events (on a table)
	Evs(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for fieds (on a table)
	Fds(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for views (on a table)
	Fts(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for indexes (on a table)
	Ixs(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for live queries (on a table)
	Lvs(NamespaceId, DatabaseId, String, Uuid),
	/// A cache key for live queries version (on a table)
	Lvv(NamespaceId, DatabaseId, String),
}

impl<'a> From<Lookup<'a>> for Key {
	fn from(value: Lookup<'a>) -> Self {
		match value {
			Lookup::Db(a, b) => Key::Db(a.to_string(), b.to_string()),
			Lookup::Tb(a, b, c) => Key::Tb(a, b, c.to_string()),
			Lookup::Evs(a, b, c, d) => Key::Evs(a, b, c.to_string(), d),
			Lookup::Fds(a, b, c, d) => Key::Fds(a, b, c.to_string(), d),
			Lookup::Fts(a, b, c, d) => Key::Fts(a, b, c.to_string(), d),
			Lookup::Ixs(a, b, c, d) => Key::Ixs(a, b, c.to_string(), d),
			Lookup::Lvs(a, b, c, d) => Key::Lvs(a, b, c.to_string(), d),
			Lookup::Lvv(a, b, c) => Key::Lvv(a, b, c.to_string()),
		}
	}
}
