use std::fmt;
use std::fmt::Display;

use crate::sql::Kind;

/// The type of records stored by a table
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl From<TableType> for crate::expr::TableType {
	fn from(v: TableType) -> Self {
		match v {
			TableType::Any => crate::expr::TableType::Any,
			TableType::Normal => crate::expr::TableType::Normal,
			TableType::Relation(rel) => crate::expr::TableType::Relation(rel.into()),
		}
	}
}

impl From<crate::expr::TableType> for TableType {
	fn from(v: crate::expr::TableType) -> Self {
		match v {
			crate::expr::TableType::Any => TableType::Any,
			crate::expr::TableType::Normal => TableType::Normal,
			crate::expr::TableType::Relation(rel) => TableType::Relation(rel.into()),
		}
	}
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Relation {
	pub from: Option<Kind>,
	pub to: Option<Kind>,
	pub enforced: bool,
}

impl From<Relation> for crate::expr::Relation {
	fn from(v: Relation) -> Self {
		crate::expr::Relation {
			from: v.from.map(Into::into),
			to: v.to.map(Into::into),
			enforced: v.enforced,
		}
	}
}

impl From<crate::expr::Relation> for Relation {
	fn from(v: crate::expr::Relation) -> Self {
		Relation {
			from: v.from.map(Into::into),
			to: v.to.map(Into::into),
			enforced: v.enforced,
		}
	}
}
