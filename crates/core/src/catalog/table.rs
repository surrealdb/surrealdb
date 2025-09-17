use revision::{Revisioned, revisioned};
use uuid::Uuid;

use crate::catalog::{DatabaseId, NamespaceId, Permissions, ViewDefinition};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{ChangeFeed, Kind};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::statements::DefineTableStatement;
use crate::sql::{Ident, ToSql};
use crate::val::{Strand, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
	pub table_type: TableType,

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
			permissions: Permissions::none(),
			changefeed: None,
			comment: None,
			table_type: TableType::default(),
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
		matches!(self.table_type, TableType::Normal | TableType::Any)
	}
	/// Checks if this table allows graph edges / relations
	pub fn allows_relation(&self) -> bool {
		matches!(self.table_type, TableType::Relation(_) | TableType::Any)
	}

	fn to_sql_definition(&self) -> DefineTableStatement {
		DefineTableStatement {
			id: Some(self.table_id.0),
			// SAFETY: we know the name is valid because it was validated when the table was
			// created.
			name: unsafe { Ident::new_unchecked(self.name.clone()) },
			drop: self.drop,
			full: self.schemafull,
			view: self.view.clone().map(|v| v.to_sql_definition()),
			permissions: self.permissions.clone().into(),
			changefeed: self.changefeed.map(|v| v.into()),
			comment: self.comment.clone().map(|v| v.into()),
			table_type: self.table_type.clone().into(),
			..Default::default()
		}
	}
}

impl ToSql for TableDefinition {
	fn to_sql(&self) -> String {
		self.to_sql_definition().to_string()
	}
}

impl InfoStructure for TableDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
			"drop".to_string() => self.drop.into(),
			"schemafull".to_string() => self.schemafull.into(),
			"kind".to_string() => self.table_type.structure(),
			"view".to_string(), if let Some(v) = self.view => v.structure(),
			"changefeed".to_string(), if let Some(v) = self.changefeed => v.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}

/// The type of records stored by a table
#[revisioned(revision = 1)]
#[derive(Debug, Default, Hash, Clone, Eq, PartialEq)]
pub enum TableType {
	#[default]
	Any,
	Normal,
	Relation(Relation),
}

impl ToSql for TableType {
	fn to_sql(&self) -> String {
		match self {
			TableType::Normal => "NORMAL".to_string(),
			TableType::Relation(rel) => {
				let mut out = "RELATION".to_string();
				if let Some(kind) = &rel.from {
					out.push_str(&format!(" IN {}", kind));
				}
				if let Some(kind) = &rel.to {
					out.push_str(&format!(" OUT {}", kind));
				}
				if rel.enforced {
					out.push_str(" ENFORCED");
				}

				out
			}
			TableType::Any => "ANY".to_string(),
		}
	}
}

impl InfoStructure for TableType {
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
					tables.into_iter().map(|t| Value::from(Strand::new_lossy(t))).collect::<Vec<_>>().into(),
				"out".to_string(), if let Some(Kind::Record(tables)) = rel.to =>
					tables.into_iter().map(|t| Value::from(Strand::new_lossy(t))).collect::<Vec<_>>().into(),
				"enforced".to_string() => rel.enforced.into()
			}),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct Relation {
	pub from: Option<Kind>,
	pub to: Option<Kind>,
	pub enforced: bool,
}
