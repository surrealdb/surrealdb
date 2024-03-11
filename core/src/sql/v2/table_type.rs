use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

use super::Kind;

/// The type of records stored by a table
#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[revisioned(revision = 1)]
pub enum TableType {
	Relation(Relation),
	Normal,
	// Should not be changed in version 2.0.0, this is required for revision compatibility
	#[default]
	Any,
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
					write!(f, " IN {}", get_tables_from_kind(kind))?;
				}
				if let Some(kind) = &rel.to {
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

fn get_tables_from_kind(kind: &Kind) -> String {
	let Kind::Record(tables) = kind else {
		panic!()
	};
	tables.iter().map(ToString::to_string).collect::<Vec<_>>().join(" | ")
}

#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[revisioned(revision = 1)]
pub struct Relation {
	pub from: Option<Kind>,
	pub to: Option<Kind>,
}
