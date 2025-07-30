
use std::fmt::Display;

use revision::{revisioned, Revisioned};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{catalog::ViewDefinition, expr::{statements::info::InfoStructure, ChangeFeed, Kind, Permissions, Value}};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct TableId(pub u32);

impl Revisioned for TableId {
    fn revision() -> u16 {
        1
    }

    #[inline]
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
        self.0.serialize_revisioned(writer)
	}

	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
        Revisioned::deserialize_revisioned(reader).map(TableId)
	}
}


#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct TableDefinition {
	pub table_id: TableId,
	pub name: String,
	pub drop: bool,
	pub schemafull: bool,
	pub view: Option<ViewDefinition>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<String>,
	pub kind: TableKind,

	/// The last time that a DEFINE FIELD was added to this table
	pub cache_fields_ts: Uuid,
	/// The last time that a DEFINE EVENT was added to this table
	pub cache_events_ts: Uuid,
	/// The last time that a DEFINE TABLE was added to this table
	pub cache_tables_ts: Uuid,
	/// The last time that a DEFINE INDEX was added to this table
	pub cache_indexes_ts: Uuid,
}

impl TableDefinition {
    /// Checks if this table allows normal records / documents
	pub fn allows_normal(&self) -> bool {
		matches!(self.kind, TableKind::Normal | TableKind::Any)
	}
    /// Checks if this table allows graph edges / relations
	pub fn allows_relation(&self) -> bool {
		matches!(self.kind, TableKind::Relation(_) | TableKind::Any)
	}
}

/// The type of records stored by a table
#[revisioned(revision = 1)]
#[derive(Debug, Default, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum TableKind {
	#[default]
	Any,
	Normal,
	Relation(Relation),
}

impl Display for TableKind {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
		match self {
			TableKind::Normal => {
				f.write_str(" NORMAL")?;
			}
			TableKind::Relation(rel) => {
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
			TableKind::Any => {
				f.write_str(" ANY")?;
			}
		}
		Ok(())
	}
}

impl InfoStructure for TableKind {
	fn structure(self) -> Value {
		match self {
			Self::Any => Value::from(map! {
				"kind".to_string() => "ANY".into(),
			}),
			Self::Normal => Value::from(map! {
				"kind".to_string() => "NORMAL".into(),
			}),
			Self::Relation(rel) => Value::from(map! {
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

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Relation {
	pub from: Option<Kind>,
	pub to: Option<Kind>,
	pub enforced: bool,
}

