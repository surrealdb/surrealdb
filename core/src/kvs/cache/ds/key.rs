use super::lookup::Lookup;

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) enum Key {
	/// A cache key for events (on a table)
	Evs(String, String, String, u64),
	/// A cache key for fieds (on a table)
	Fds(String, String, String, u64),
	/// A cache key for views (on a table)
	Fts(String, String, String, u64),
	/// A cache key for indexes (on a table)
	Ixs(String, String, String, u64),
	/// A cache key for live queries (on a table)
	Lvs(String, String, String, u64),
}

impl<'a> From<Lookup<'a>> for Key {
	#[rustfmt::skip]
	fn from(value: Lookup<'a>) -> Self {
		match value {
			Lookup::Evs(a, b, c, d) => Key::Evs(a.to_string(), b.to_string(), c.to_string(), d),
			Lookup::Fds(a, b, c, d) => Key::Fds(a.to_string(), b.to_string(), c.to_string(), d),
			Lookup::Fts(a, b, c, d) => Key::Fts(a.to_string(), b.to_string(), c.to_string(), d),
			Lookup::Ixs(a, b, c, d) => Key::Ixs(a.to_string(), b.to_string(), c.to_string(), d),
			Lookup::Lvs(a, b, c, d) => Key::Lvs(a.to_string(), b.to_string(), c.to_string(), d),
		}
	}
}
