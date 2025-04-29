use super::lookup::Lookup;
use uuid::Uuid;

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) enum Key {
	/// A cache key for a database
	Db(String, String),
	/// A cache key for a table
	Tb(String, String, String),
	/// A cache key for events (on a table)
	Evs(String, String, String, Uuid),
	/// A cache key for fieds (on a table)
	Fds(String, String, String, Uuid),
	/// A cache key for views (on a table)
	Fts(String, String, String, Uuid),
	/// A cache key for indexes (on a table)
	Ixs(String, String, String, Uuid),
	/// A cache key for live queries (on a table)
	Lvs(String, String, String, Uuid),
	/// A cache key for live queries version (on a table)
	Lvv(String, String, String),
}

impl<'a> From<Lookup<'a>> for Key {
	#[rustfmt::skip]
	fn from(value: Lookup<'a>) -> Self {
		match value {
			Lookup::Db(a, b) => Key::Db(a.to_string(), b.to_string()),
			Lookup::Tb(a, b, c) => Key::Tb(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Evs(a, b, c, d) => Key::Evs(a.to_string(), b.to_string(), c.to_string(), d),
			Lookup::Fds(a, b, c, d) => Key::Fds(a.to_string(), b.to_string(), c.to_string(), d),
			Lookup::Fts(a, b, c, d) => Key::Fts(a.to_string(), b.to_string(), c.to_string(), d),
			Lookup::Ixs(a, b, c, d) => Key::Ixs(a.to_string(), b.to_string(), c.to_string(), d),
			Lookup::Lvs(a, b, c, d) => Key::Lvs(a.to_string(), b.to_string(), c.to_string(), d),
			Lookup::Lvv(a, b, c) => Key::Lvv(a.to_string(), b.to_string(), c.to_string()),
		}
	}
}
