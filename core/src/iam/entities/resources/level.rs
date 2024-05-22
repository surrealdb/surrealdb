use revision::revisioned;
use std::{
	collections::{HashMap, HashSet},
	str::FromStr,
};

use cedar_policy::{Entity, EntityTypeName, EntityUid, RestrictedExpression};
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Deserialize, Serialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
	pub fn level_name(&self) -> &str {
		match self {
			Level::No => "No",
			Level::Root => "Root",
			Level::Namespace(_) => "Namespace",
			Level::Database(_, _) => "Database",
			Level::Record(_, _, _) => "Record",
		}
	}

	pub fn ns(&self) -> Option<&str> {
		match self {
			Level::Namespace(ns) => Some(ns),
			Level::Database(ns, _) => Some(ns),
			Level::Record(ns, _, _) => Some(ns),
			_ => None,
		}
	}

	pub fn db(&self) -> Option<&str> {
		match self {
			Level::Database(_, db) => Some(db),
			Level::Record(_, db, _) => Some(db),
			_ => None,
		}
	}

	pub fn id(&self) -> Option<&str> {
		match self {
			Level::Record(_, _, id) => Some(id),
			_ => None,
		}
	}

	fn parent(&self) -> Option<Level> {
		match self {
			Level::No => None,
			Level::Root => None,
			Level::Namespace(_) => Some(Level::Root),
			Level::Database(ns, _) => Some(Level::Namespace(ns.to_owned())),
			Level::Record(ns, db, _) => Some(Level::Database(ns.to_owned(), db.to_owned())),
		}
	}

	// Cedar policy helpers
	pub fn cedar_attrs(&self) -> HashMap<String, RestrictedExpression> {
		let mut attrs = HashMap::with_capacity(5);
		attrs.insert("type".into(), RestrictedExpression::new_string(self.level_name().to_owned()));

		if let Some(ns) = self.ns() {
			attrs.insert("ns".into(), RestrictedExpression::new_string(ns.to_owned()));
		}

		if let Some(db) = self.db() {
			attrs.insert("db".into(), RestrictedExpression::new_string(db.to_owned()));
		}

		if let Some(id) = self.id() {
			attrs.insert("id".into(), RestrictedExpression::new_string(id.to_owned()));
		}

		attrs
	}

	pub fn cedar_parents(&self) -> HashSet<EntityUid> {
		if let Some(parent) = self.parent() {
			return HashSet::from([parent.into()]);
		}
		HashSet::with_capacity(0)
	}

	pub fn cedar_entities(&self) -> Vec<Entity> {
		let mut entities = Vec::new();

		entities.push(self.into());

		// Find all the parents
		let mut parent = self.parent();
		while let Some(p) = parent {
			parent = p.parent();
			entities.push(p.into());
		}

		entities
	}
}

impl From<()> for Level {
	fn from(_: ()) -> Self {
		Level::Root
	}
}

impl From<(&str,)> for Level {
	fn from((ns,): (&str,)) -> Self {
		Level::Namespace(ns.to_owned())
	}
}

impl From<(&str, &str)> for Level {
	fn from((ns, db): (&str, &str)) -> Self {
		Level::Database(ns.to_owned(), db.to_owned())
	}
}

impl From<(&str, &str, &str)> for Level {
	fn from((ns, db, id): (&str, &str, &str)) -> Self {
		Level::Record(ns.to_owned(), db.to_owned(), id.to_owned())
	}
}

impl From<(Option<&str>, Option<&str>, Option<&str>)> for Level {
	fn from(val: (Option<&str>, Option<&str>, Option<&str>)) -> Self {
		match val {
			(None, None, None) => ().into(),
			(Some(ns), None, None) => (ns,).into(),
			(Some(ns), Some(db), None) => (ns, db).into(),
			(Some(ns), Some(db), Some(id)) => (ns, db, id).into(),
			_ => Level::No,
		}
	}
}

impl std::convert::From<Level> for EntityUid {
	fn from(level: Level) -> Self {
		EntityUid::from_type_name_and_id(
			EntityTypeName::from_str("Level").unwrap(),
			format!("{}", level).parse().unwrap(),
		)
	}
}

impl std::convert::From<&Level> for EntityUid {
	fn from(level: &Level) -> Self {
		level.to_owned().into()
	}
}

impl std::convert::From<Level> for Entity {
	fn from(level: Level) -> Self {
		Entity::new(level.to_owned().into(), level.cedar_attrs(), level.cedar_parents())
	}
}

impl std::convert::From<&Level> for Entity {
	fn from(level: &Level) -> Self {
		level.to_owned().into()
	}
}

impl std::convert::From<Level> for RestrictedExpression {
	fn from(level: Level) -> Self {
		format!("{}", EntityUid::from(level)).parse().unwrap()
	}
}

impl std::convert::From<&Level> for RestrictedExpression {
	fn from(level: &Level) -> Self {
		level.to_owned().into()
	}
}
