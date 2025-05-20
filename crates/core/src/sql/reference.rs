use anyhow::{Result, bail};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::{
	ctx::Context,
	dbs::{Options, capabilities::ExperimentalTarget},
	doc::CursorDoc,
	err::Error,
};

use super::{Array, Idiom, Table, Thing, SqlValue, array::Uniq};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::Reference")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Reference {
	pub on_delete: ReferenceDeleteStrategy,
}

impl fmt::Display for Reference {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ON DELETE {}", &self.on_delete)
	}
}

impl From<Reference> for crate::expr::reference::Reference {
	fn from(v: Reference) -> Self {
		Self {
			on_delete: v.on_delete.into(),
		}
	}
}
impl From<crate::expr::reference::Reference> for Reference {
	fn from(v: crate::expr::reference::Reference) -> Self {
		Self {
			on_delete: v.on_delete.into(),
		}
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
	Custom(SqlValue),
}

impl fmt::Display for ReferenceDeleteStrategy {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			ReferenceDeleteStrategy::Reject => write!(f, "REJECT"),
			ReferenceDeleteStrategy::Ignore => write!(f, "IGNORE"),
			ReferenceDeleteStrategy::Cascade => write!(f, "CASCADE"),
			ReferenceDeleteStrategy::Unset => write!(f, "UNSET"),
			ReferenceDeleteStrategy::Custom(v) => write!(f, "THEN {}", v),
		}
	}
}

impl From<ReferenceDeleteStrategy> for crate::expr::reference::ReferenceDeleteStrategy {
	fn from(v: ReferenceDeleteStrategy) -> Self {
		match v {
			ReferenceDeleteStrategy::Reject => {
				crate::expr::reference::ReferenceDeleteStrategy::Reject
			}
			ReferenceDeleteStrategy::Ignore => {
				crate::expr::reference::ReferenceDeleteStrategy::Ignore
			}
			ReferenceDeleteStrategy::Cascade => {
				crate::expr::reference::ReferenceDeleteStrategy::Cascade
			}
			ReferenceDeleteStrategy::Unset => {
				crate::expr::reference::ReferenceDeleteStrategy::Unset
			}
			ReferenceDeleteStrategy::Custom(v) => {
				crate::expr::reference::ReferenceDeleteStrategy::Custom(v.into())
			}
		}
	}
}

impl From<crate::expr::reference::ReferenceDeleteStrategy> for ReferenceDeleteStrategy {
	fn from(v: crate::expr::reference::ReferenceDeleteStrategy) -> Self {
		match v {
			crate::expr::reference::ReferenceDeleteStrategy::Reject => {
				ReferenceDeleteStrategy::Reject
			}
			crate::expr::reference::ReferenceDeleteStrategy::Ignore => {
				ReferenceDeleteStrategy::Ignore
			}
			crate::expr::reference::ReferenceDeleteStrategy::Cascade => {
				ReferenceDeleteStrategy::Cascade
			}
			crate::expr::reference::ReferenceDeleteStrategy::Unset => {
				ReferenceDeleteStrategy::Unset
			}
			crate::expr::reference::ReferenceDeleteStrategy::Custom(v) => {
				ReferenceDeleteStrategy::Custom(v.into())
			}
		}
	}
}


#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, PartialOrd)]
#[serde(rename = "$surrealdb::private::sql::Refs")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Refs(pub Vec<(Option<Table>, Option<Idiom>)>);

impl fmt::Display for Refs {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "[]")
	}
}

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