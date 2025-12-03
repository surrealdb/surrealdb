use surrealdb_types::{SqlFormat, ToSql, write_sql};

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

impl ToSql for TableType {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			TableType::Normal => {
				write_sql!(f, sql_fmt, " NORMAL");
			}
			TableType::Relation(rel) => {
				write_sql!(f, sql_fmt, " RELATION");
				if let Some(kind) = &rel.from {
					write_sql!(f, sql_fmt, " IN {kind}");
				}
				if let Some(kind) = &rel.to {
					write_sql!(f, sql_fmt, " OUT {kind}");
				}
				if rel.enforced {
					write_sql!(f, sql_fmt, " ENFORCED");
				}
			}
			TableType::Any => {
				write_sql!(f, sql_fmt, " ANY");
			}
		}
	}
}

impl From<TableType> for crate::catalog::TableType {
	fn from(v: TableType) -> Self {
		match v {
			TableType::Any => Self::Any,
			TableType::Normal => Self::Normal,
			TableType::Relation(rel) => Self::Relation(rel.into()),
		}
	}
}

impl From<crate::catalog::TableType> for TableType {
	fn from(v: crate::catalog::TableType) -> Self {
		match v {
			crate::catalog::TableType::Any => Self::Any,
			crate::catalog::TableType::Normal => Self::Normal,
			crate::catalog::TableType::Relation(rel) => Self::Relation(rel.into()),
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
