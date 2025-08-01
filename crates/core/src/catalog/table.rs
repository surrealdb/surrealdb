use crate::{
	catalog::{DatabaseId, NamespaceId},
	kvs::impl_kv_value_revisioned,
	sql::ToSql,
};
use revision::{Revisioned, revisioned};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
	catalog::ViewDefinition,
	expr::{ChangeFeed, Kind, Permissions, Value, statements::info::InfoStructure},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct TableId(pub u32);

impl_kv_value_revisioned!(TableId);

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
	pub namespace_id: NamespaceId,
	pub database_id: DatabaseId,
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

impl_kv_value_revisioned!(TableDefinition);

impl TableDefinition {
	pub fn new(
		namespace_id: NamespaceId,
		database_id: DatabaseId,
		table_id: TableId,
		name: String,
	) -> Self {
		let now = Uuid::now_v7();
		Self {
			namespace_id,
			database_id,
			table_id,
			name,
			drop: false,
			schemafull: false,
			view: None,
			permissions: Permissions::default(),
			changefeed: None,
			comment: None,
			kind: TableKind::Normal,
			cache_fields_ts: now,
			cache_events_ts: now,
			cache_tables_ts: now,
			cache_indexes_ts: now,
		}
	}

	pub fn with_changefeed(mut self, changefeed: ChangeFeed) -> Self {
		self.changefeed = Some(changefeed);
		self
	}

	/// Checks if this table allows normal records / documents
	pub fn allows_normal(&self) -> bool {
		matches!(self.kind, TableKind::Normal | TableKind::Any)
	}
	/// Checks if this table allows graph edges / relations
	pub fn allows_relation(&self) -> bool {
		matches!(self.kind, TableKind::Relation(_) | TableKind::Any)
	}
}

impl ToSql for TableDefinition {
	fn to_sql(&self) -> String {
		format!("DEFINE TABLE {} {}", self.name, self.kind.to_sql())
	}
}

impl InfoStructure for TableDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"drop".to_string() => self.drop.into(),
			"schemafull".to_string() => self.schemafull.into(),
			"kind".to_string() => self.kind.structure(),
			"view".to_string(), if let Some(v) = self.view => v.structure(),
			"changefeed".to_string(), if let Some(v) = self.changefeed => v.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
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

impl ToSql for TableKind {
	fn to_sql(&self) -> String {
		let mut sql = String::new();
		match self {
			TableKind::Normal => {
				sql.push_str("NORMAL");
			}
			TableKind::Relation(rel) => {
				sql.push_str("RELATION");
				if let Some(kind) = &rel.from {
					sql.push_str(&format!(" IN {}", kind.to_sql()));
				}
				if let Some(kind) = &rel.to {
					sql.push_str(&format!(" OUT {}", kind.to_sql()));
				}
				if rel.enforced {
					sql.push_str(" ENFORCED");
				}
			}
			TableKind::Any => {
				sql.push_str("ANY");
			}
		}
		sql
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
