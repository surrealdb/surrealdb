use crate::sql::statements::info::InfoStructure;
use crate::sql::Array;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

use super::{Kind, Object, Table, Value};

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
				if let Some(Kind::Record(kind)) = &rel.from {
					write!(f, " IN {}", get_tables_from_kind(kind).join(" | "))?;
				}
				if let Some(Kind::Record(kind)) = &rel.to {
					write!(f, " OUT {}", get_tables_from_kind(kind).join(" | "))?;
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
		let mut acc = Object::default();

		match &self {
			TableType::Any => {
				acc.insert("kind".to_string(), "ANY".into());
			}
			TableType::Normal => {
				acc.insert("kind".to_string(), "NORMAL".into());
			}
			TableType::Relation(rel) => {
				acc.insert("kind".to_string(), "RELATION".into());

				if let Some(Kind::Record(tables)) = &rel.from {
					acc.insert(
						"in".to_string(),
						Value::Array(Array::from(get_tables_from_kind(tables))),
					);
				}

				if let Some(Kind::Record(tables)) = &rel.to {
					acc.insert(
						"out".to_string(),
						Value::Array(Array::from(get_tables_from_kind(tables))),
					);
				}
			}
		};

		Value::Object(acc)
	}
}

fn get_tables_from_kind(tables: &[Table]) -> Vec<&str> {
	tables.iter().map(|t| t.0.as_str()).collect::<Vec<_>>()
}

#[revisioned(revision = 1)]
#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Relation {
	pub from: Option<Kind>,
	pub to: Option<Kind>,
}
