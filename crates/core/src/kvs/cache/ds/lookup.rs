use super::key::Key;
use quick_cache::Equivalent;
use uuid::Uuid;

#[derive(Hash, Eq, PartialEq)]
pub(crate) enum Lookup<'a> {
	/// A cache key for a database
	Db(&'a str, &'a str),
	/// A cache key for a table
	Tb(&'a str, &'a str, &'a str),
	/// A cache key for events (on a table)
	Evs(&'a str, &'a str, &'a str, Uuid),
	/// A cache key for fields (on a table)
	Fds(&'a str, &'a str, &'a str, Uuid),
	/// A cache key for views (on a table)
	Fts(&'a str, &'a str, &'a str, Uuid),
	/// A cache key for indexes (on a table)
	Ixs(&'a str, &'a str, &'a str, Uuid),
	/// A cache key for live queries (on a table)
	Lvs(&'a str, &'a str, &'a str, Uuid),
	/// A cache key for live queries version (on a table)
	Lvv(&'a str, &'a str, &'a str),
}

impl Equivalent<Key> for Lookup<'_> {
	#[rustfmt::skip]
	fn equivalent(&self, key: &Key) -> bool {
		match (self, key) {
			(Self::Db(la, lb), Key::Db(ka, kb)) => la == ka && lb == kb,
			(Self::Tb(la, lb, lc), Key::Tb(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Evs(la, lb, lc, ld), Key::Evs(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Fds(la, lb, lc, ld), Key::Fds(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Fts(la, lb, lc, ld), Key::Fts(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Ixs(la, lb, lc, ld), Key::Ixs(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Lvs(la, lb, lc, ld), Key::Lvs(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Lvv(la, lb, lc), Key::Lvv(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			_ => false,
		}
	}
}
