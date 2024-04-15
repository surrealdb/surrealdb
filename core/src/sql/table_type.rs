use crate::sql::array::Concat;
use crate::sql::statements::info::InfoStructure;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

use super::{Kind, Table, Value};

/// The type of records stored by a table
#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
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
				if let Some(Kind::Record(kind)) = &rel.from {
					write!(f, " IN {}", get_tables_from_kind(kind))?;
				}
				if let Some(Kind::Record(kind)) = &rel.to {
					write!(f, " OUT {}", get_tables_from_kind(kind))?;
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
			TableType::Normal => "NORMAL".to_string(),
			TableType::Relation(rel) => {
				let mut acc = "RELATION".to_string();
				if let Some(Kind::Record(kind)) = &rel.from {
					acc = acc.concat(format!(" IN {}", get_tables_from_kind(kind)));
				}
				if let Some(Kind::Record(kind)) = &rel.to {
					acc = acc.concat(format!(" OUT {}", get_tables_from_kind(kind)));
				}
				acc
			}
			TableType::Any => "ANY".to_string(),
		}
		.into()
	}
}

fn get_tables_from_kind(tables: &[Table]) -> String {
	tables.iter().map(|t| t.0.as_str()).collect::<Vec<_>>().join(" | ")
}

#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
#[non_exhaustive]
pub struct Relation {
	pub from: Option<Kind>,
	pub to: Option<Kind>,
}
