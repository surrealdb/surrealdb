use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Deserialize, Serialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Level {
	#[default]
	No,
	Root,
	Namespace(String),
	Database(String, String),
	Record(String, String, String),
}

impl std::fmt::Display for Level {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Level::No => write!(f, "No"),
			Level::Root => write!(f, "/"),
			Level::Namespace(ns) => write!(f, "/ns:{ns}/"),
			Level::Database(ns, db) => write!(f, "/ns:{ns}/db:{db}/"),
			Level::Record(ns, db, id) => write!(f, "/ns:{ns}/db:{db}/id:{id}/"),
		}
	}
}

impl Level {
	/// Returns if the level is a sub level of the given level.
	/// For example Level::Namespace is a sublevel of Level::Root, and
	/// Level::Database("foo", "bar") is a sublevel of Level::Namespace("foo").
	/// Every level is also a sublevel of itself.
	pub(crate) fn sublevel_of(&self, other: &Self) -> bool {
		match self {
			Level::No => true,
			Level::Root => matches!(other, Level::Root),
			Level::Namespace(a) => match other {
				Level::Root => true,
				Level::Namespace(b) => a == b,
				_ => false,
			},
			Level::Database(ns0, db0) => match other {
				Level::Root => true,
				Level::Namespace(ns1) => ns0 == ns1,
				Level::Database(ns1, db1) => ns0 == ns1 && db0 == db1,
				_ => false,
			},
			Level::Record(ns0, db0, ac0) => match other {
				Level::Root => true,
				Level::Namespace(ns1) => ns0 == ns1,
				Level::Database(ns1, db1) => ns0 == ns1 && db0 == db1,
				Level::Record(ns1, db1, ac1) => ns0 == ns1 && db0 == db1 && ac0 == ac1,
				_ => false,
			},
		}
	}

	pub(crate) fn ns(&self) -> Option<&str> {
		match self {
			Level::Namespace(ns) => Some(ns),
			Level::Database(ns, _) => Some(ns),
			Level::Record(ns, _, _) => Some(ns),
			_ => None,
		}
	}

	pub(crate) fn db(&self) -> Option<&str> {
		match self {
			Level::Database(_, db) => Some(db),
			Level::Record(_, db, _) => Some(db),
			_ => None,
		}
	}

	#[cfg(test)]
	pub(crate) fn id(&self) -> Option<&str> {
		match self {
			Level::Record(_, _, id) => Some(id),
			_ => None,
		}
	}
}
