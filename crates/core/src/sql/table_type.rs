use crate::sql::Kind;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// The type of records stored by a table
#[revisioned(revision = 1)]
#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum TableType {
	#[default]
	Any,
	Normal,
	Relation(Relation),
}

impl Display for TableType {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			TableType::Normal => {
				f.write_str(" NORMAL")?;
			}
			TableType::Relation(rel) => {
				f.write_str(" RELATION")?;
				if let Some(kind) = &rel.from {
					write!(f, " IN {kind}")?;
				}
				if let Some(kind) = &rel.to {
					write!(f, " OUT {kind}")?;
				}
				if rel.enforced {
					write!(f, " ENFORCED")?;
				}
			}
			TableType::Any => {
				f.write_str(" ANY")?;
			}
		}
		Ok(())
	}
}

impl From<TableType> for crate::catalog::TableKind {
	fn from(v: TableType) -> Self {
		match v {
			TableType::Any => Self::Any,
			TableType::Normal => Self::Normal,
			TableType::Relation(rel) => Self::Relation(rel.into()),
		}
	}
}

impl From<crate::catalog::TableKind> for TableType {
	fn from(v: crate::catalog::TableKind) -> Self {
		match v {
			crate::catalog::TableKind::Any => Self::Any,
			crate::catalog::TableKind::Normal => Self::Normal,
			crate::catalog::TableKind::Relation(rel) => Self::Relation(rel.into()),
		}
	}
}

#[revisioned(revision = 2)]
#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Relation {
	pub from: Option<Kind>,
	pub to: Option<Kind>,
	#[revision(start = 2)]
	pub enforced: bool,
}

impl From<Relation> for crate::catalog::Relation {
	fn from(v: Relation) -> Self {
		Self {
			from: v.from.map(Into::into),
			to: v.to.map(Into::into),
			enforced: v.enforced,
		}
	}
}

impl From<crate::catalog::Relation> for Relation {
	fn from(v: crate::catalog::Relation) -> Self {
		Self {
			from: v.from.map(Into::into),
			to: v.to.map(Into::into),
			enforced: v.enforced,
		}
	}
}
