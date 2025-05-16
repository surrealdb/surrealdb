use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::{
	ctx::Context,
	dbs::{capabilities::ExperimentalTarget, Options},
	doc::CursorDoc,
	err::Error,
};

use super::{array::Uniq, statements::info::InfoStructure, Array, Idiom, Table, Thing, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::Reference")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Reference {
	pub on_delete: ReferenceDeleteStrategy,
}

crate::sql::impl_display_from_sql!(Reference);

impl crate::sql::DisplaySql for Reference {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ON DELETE {}", &self.on_delete)
	}
}

impl InfoStructure for Reference {
	fn structure(self) -> Value {
		map! {
			"on_delete" => self.on_delete.structure(),
		}
		.into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::ReferenceDeleteStrategy")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum ReferenceDeleteStrategy {
	Reject,
	Ignore,
	Cascade,
	Unset,
	Custom(Value),
}

crate::sql::impl_display_from_sql!(ReferenceDeleteStrategy);

impl crate::sql::DisplaySql for ReferenceDeleteStrategy {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			ReferenceDeleteStrategy::Reject => write!(f, "REJECT"),
			ReferenceDeleteStrategy::Ignore => write!(f, "IGNORE"),
			ReferenceDeleteStrategy::Cascade => write!(f, "CASCADE"),
			ReferenceDeleteStrategy::Unset => write!(f, "UNSET"),
			ReferenceDeleteStrategy::Custom(v) => write!(f, "THEN {}", v),
		}
	}
}

impl InfoStructure for ReferenceDeleteStrategy {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::Refs")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Refs(pub Vec<(Option<Table>, Option<Idiom>)>);

impl From<Refs> for crate::expr::reference::Refs {
	fn from(v: Refs) -> Self {
		Self(v.0.into_iter().map(|(t, i)| (t.map(Into::into), i.map(Into::into))).collect())
	}
}

impl From<crate::expr::reference::Refs> for Refs {
	fn from(v: crate::expr::reference::Refs) -> Self {
		Self(v.0.into_iter().map(|(t, i)| (t.map(Into::into), i.map(Into::into))).collect())
	}
}

crate::sql::impl_display_from_sql!(Refs);

impl crate::sql::DisplaySql for Refs {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "[]")
	}
}
