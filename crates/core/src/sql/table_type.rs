use crate::sql::statements::info::InfoStructure;
use crate::sql::{Kind, Value};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

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

crate::sql::impl_display_from_sql!(TableType);

impl crate::sql::DisplaySql for TableType {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

impl InfoStructure for TableType {
	fn structure(self) -> Value {
		match self {
			TableType::Any => Value::from(map! {
				"kind".to_string() => "ANY".into(),
			}),
			TableType::Normal => Value::from(map! {
				"kind".to_string() => "NORMAL".into(),
			}),
			TableType::Relation(rel) => Value::from(map! {
				"kind".to_string() => "RELATION".into(),
				"in".to_string(), if let Some(Kind::Record(tables)) = rel.from =>
					tables.into_iter().map(|t| t.0).collect::<Vec<_>>().into(),
				"out".to_string(), if let Some(Kind::Record(tables)) = rel.to =>
					tables.into_iter().map(|t| t.0).collect::<Vec<_>>().into(),
				"enforced".to_string() => rel.enforced.into()
			}),
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