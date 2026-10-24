//! The record-link columns of the tables an SQL statement names.
//!
//! The PostgreSQL and MySQL front ends read and write a column declared
//! `record<T>` by its bare key: a comparison against `1` matches the link
//! `T:1`, and an assignment of `1` stores it. They can only do that when the
//! lowering is told which columns are links, and a catalog read is `async`
//! where the lowering is not, so a request is lowered twice at most.
//!
//! 1. The statements are lowered against a [`RecordingCatalog`], which describes nothing and notes
//!    each table the lowering asks about.
//! 2. When it noted tables, the field definitions of those tables are read ([`link_columns`]). If
//!    none of them declares a link, the first lowering already is the answer; otherwise the
//!    statements are lowered again against a catalog that names the links.
//!
//! A request that names no table (`SELECT 1`) reads nothing, and neither does one whose session the
//! datastore refuses or whose level cannot reach the selected database: `Datastore::link_columns`
//! decides whether [`link_columns`] runs.

use std::collections::BTreeSet;
use std::sync::{Mutex, PoisonError};

use anyhow::Result;
use surrealdb_sqlplan::{BinderCatalog, EmptyCatalog, MemoryCatalog, TableSchema};

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::expr::Kind;
use crate::kvs::Transaction;
use crate::val::TableName;

/// A catalog that describes nothing, as [`EmptyCatalog`] does, and notes every
/// table the lowering asks it about.
#[derive(Default)]
pub(super) struct RecordingCatalog {
	tables: Mutex<BTreeSet<String>>,
}

impl RecordingCatalog {
	fn note(&self, table: &str) {
		self.tables.lock().unwrap_or_else(PoisonError::into_inner).insert(table.to_owned());
	}

	/// The tables asked about, in name order.
	pub(super) fn into_tables(self) -> Vec<String> {
		let tables = self.tables.into_inner().unwrap_or_else(PoisonError::into_inner);
		tables.into_iter().collect()
	}
}

impl BinderCatalog for RecordingCatalog {
	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema> {
		let system = EmptyCatalog.get_table(schema, name);
		if system.is_none() {
			self.note(name);
		}
		system
	}

	fn link_table(&self, _schema: Option<&str>, table: &str, _column: &str) -> Option<String> {
		self.note(table);
		None
	}
}

/// The table a field of this type links to, when the type names exactly one
/// table and admits nothing but a link besides `NONE` and `NULL`:
/// `record<t>`, `option<record<t>>` and `record<t> | none`.
fn link_target(kind: &Kind) -> Option<&str> {
	match kind {
		Kind::Record(tables) => match tables.as_slice() {
			[table] => Some(table.as_str()),
			_ => None,
		},
		Kind::Either(kinds) => {
			let mut target = None;
			for kind in kinds {
				if matches!(kind, Kind::None | Kind::Null) {
					continue;
				}
				if target.is_some() {
					return None;
				}
				target = Some(link_target(kind)?);
			}
			target
		}
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Decimal
		| Kind::Duration
		| Kind::Float
		| Kind::F16
		| Kind::F32
		| Kind::Int
		| Kind::I8
		| Kind::I16
		| Kind::I32
		| Kind::U8
		| Kind::U16
		| Kind::U32
		| Kind::U64
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Array(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_) => None,
	}
}

/// The link columns of `tables` as `(table, column, target)`, read through
/// `txn` from the database `ns`/`db`. A database that does not exist has none.
pub(super) async fn link_columns(
	txn: &Transaction,
	ns: &str,
	db: &str,
	tables: &[String],
) -> Result<Vec<(String, String, String)>> {
	let Some(database) = txn.get_db_by_name(ns, db, None).await? else {
		return Ok(Vec::new());
	};
	let mut links = Vec::new();
	for table in tables {
		let name = TableName::from(table.as_str());
		let fields = txn.all_tb_fields(database.namespace_id, database.database_id, &name, None);
		for field in fields.await?.iter() {
			if field.is_key_part() {
				continue;
			}
			let (Some(column), Some(kind)) = (field.name.field_name(), &field.field_kind) else {
				continue;
			};
			if let Some(target) = link_target(kind) {
				links.push((table.clone(), column.to_owned(), target.to_owned()));
			}
		}
	}
	Ok(links)
}

/// A catalog naming `links`, as [`link_columns`] returns them, or `None` when
/// there are none.
pub(super) fn link_catalog(links: &[(String, String, String)]) -> Option<MemoryCatalog> {
	if links.is_empty() {
		return None;
	}
	Some(links.iter().fold(MemoryCatalog::new(), |catalog, (table, column, target)| {
		catalog.with_link(table, column, target)
	}))
}

/// The SQL type of a field of `kind`, where it has one wire representation. Optionality does not
/// change the type, and an array of such a scalar is an array of its type. Mixed unions, record
/// links, sets, nested arrays and arrays of anything else are [`SqlType::Unknown`], typed from the
/// values they hold.
///
/// [`SqlType::Unknown`]: surrealdb_sqlplan::SqlType::Unknown
#[cfg(feature = "postgres")]
fn scalar_type(kind: &Kind) -> surrealdb_sqlplan::SqlType {
	use surrealdb_sqlplan::SqlType;
	match kind {
		Kind::Bool => SqlType::Bool,
		Kind::Bytes => SqlType::Bytea,
		Kind::Datetime => SqlType::TimestampTz,
		Kind::Decimal => SqlType::Numeric,
		Kind::Duration => SqlType::Interval,
		Kind::Float => SqlType::Float8,
		Kind::F16 => SqlType::Float4,
		Kind::F32 => SqlType::Float4,
		Kind::Int => SqlType::Int8,
		Kind::I8 => SqlType::Int2,
		Kind::I16 => SqlType::Int2,
		Kind::I32 => SqlType::Int4,
		Kind::U8 => SqlType::Int2,
		Kind::U16 => SqlType::Int4,
		Kind::U32 => SqlType::Int8,
		Kind::U64 => SqlType::Numeric,
		Kind::String => SqlType::Text,
		Kind::Uuid => SqlType::Uuid,
		Kind::Object => SqlType::Json,
		Kind::Array(elem, _) => SqlType::array_of(scalar_type(elem)).unwrap_or(SqlType::Unknown),
		Kind::Either(kinds) => {
			let mut types = kinds
				.iter()
				.filter(|kind| !matches!(kind, Kind::None | Kind::Null))
				.map(scalar_type);
			match types.next() {
				Some(ty) if types.all(|other| other == ty) => ty,
				_ => SqlType::Unknown,
			}
		}
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Number
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_) => SqlType::Unknown,
	}
}

#[cfg(feature = "postgres")]
pub(super) async fn column_types(
	txn: &Transaction,
	ns: &str,
	db: &str,
	tables: &[String],
) -> Result<MemoryCatalog> {
	let mut catalog = MemoryCatalog::new();
	catalog.set_database(Some(db.to_owned()));
	let Some(database) = txn.get_db_by_name(ns, db, None).await? else {
		return Ok(catalog);
	};
	for table in tables {
		let fields = txn
			.all_tb_fields(
				database.namespace_id,
				database.database_id,
				&TableName::from(table.as_str()),
				None,
			)
			.await?;
		for field in fields.iter() {
			let (Some(column), Some(kind)) = (field.name.field_name(), &field.field_kind) else {
				continue;
			};
			let ty = scalar_type(kind);
			if ty != surrealdb_sqlplan::SqlType::Unknown {
				catalog = catalog.with_column_type(table, column, ty);
			}
		}
	}
	Ok(catalog)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn record(table: &str) -> Kind {
		Kind::Record(vec![TableName::from(table)])
	}

	#[test]
	fn a_link_is_a_type_naming_one_table_and_nothing_else() {
		assert_eq!(link_target(&record("dept")), Some("dept"));
		assert_eq!(link_target(&Kind::Either(vec![Kind::None, record("dept")])), Some("dept"));
		assert_eq!(link_target(&Kind::Either(vec![record("dept"), Kind::Null])), Some("dept"));
		assert_eq!(
			link_target(&Kind::Either(vec![Kind::None, Kind::Either(vec![record("dept")])])),
			Some("dept")
		);
	}

	#[test]
	fn other_types_are_not_links() {
		assert_eq!(link_target(&Kind::Record(Vec::new())), None);
		assert_eq!(link_target(&Kind::Record(vec!["a".into(), "b".into()])), None);
		assert_eq!(link_target(&Kind::Either(vec![record("a"), record("b")])), None);
		assert_eq!(link_target(&Kind::Either(vec![record("dept"), Kind::Int])), None);
		assert_eq!(link_target(&Kind::Either(vec![Kind::Int, record("dept")])), None);
		assert_eq!(link_target(&Kind::Array(Box::new(record("dept")), None)), None);
		assert_eq!(link_target(&Kind::Any), None);
		assert_eq!(link_target(&Kind::Either(vec![Kind::None, Kind::Null])), None);
	}

	#[cfg(feature = "postgres")]
	#[test]
	fn an_array_of_a_scalar_is_typed_and_other_collections_are_not() {
		use surrealdb_sqlplan::SqlType;
		let array = |kind: Kind| Kind::Array(Box::new(kind), None);
		assert_eq!(scalar_type(&array(Kind::String)), SqlType::Array(&SqlType::Text));
		assert_eq!(scalar_type(&array(Kind::Int)), SqlType::Array(&SqlType::Int8));
		assert_eq!(
			scalar_type(&Kind::Either(vec![Kind::None, array(Kind::String)])),
			SqlType::Array(&SqlType::Text)
		);
		assert_eq!(scalar_type(&array(array(Kind::String))), SqlType::Unknown);
		assert_eq!(scalar_type(&array(Kind::Any)), SqlType::Unknown);
		assert_eq!(scalar_type(&array(record("dept"))), SqlType::Unknown);
		assert_eq!(scalar_type(&Kind::Set(Box::new(Kind::String), None)), SqlType::Unknown);
	}

	#[test]
	fn the_recording_catalog_notes_tables_but_not_system_ones() {
		let catalog = RecordingCatalog::default();
		assert!(catalog.get_table(None, "emp").is_none());
		assert_eq!(catalog.link_table(None, "emp", "dept"), None);
		assert_eq!(catalog.link_table(None, "dept", "boss"), None);
		assert_eq!(catalog.into_tables(), vec!["dept".to_owned(), "emp".to_owned()]);
	}
}
