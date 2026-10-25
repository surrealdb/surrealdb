//! What the datastore knows of the tables an SQL statement names: the record-link columns, the
//! declared columns and their types.
//!
//! The PostgreSQL and MySQL front ends read and write a column declared
//! `record<T>` by its bare key: a comparison against `1` matches the link
//! `T:1`, and an assignment of `1` stores it. A table declared `SCHEMAFULL` has the
//! columns it defines and no others, so a name outside them is an error and `*` is those columns.
//! The front ends can only do that when the lowering is told about the tables, and a catalog read
//! is `async` where the lowering is not, so a request is lowered twice at most.
//!
//! 1. The statements are lowered against a [`RecordingCatalog`], which describes nothing and notes
//!    each table the lowering asks about.
//! 2. When it noted tables, those tables are read: for the links alone ([`link_columns`]), or for
//!    the links and the declared columns ([`describe_tables`]). If nothing they declare changes how
//!    a statement lowers, the first lowering already is the answer; otherwise the statements are
//!    lowered again against a catalog that describes the tables.
//!
//! A request that names no table (`SELECT 1`) reads nothing, and neither does one whose session the
//! datastore refuses or whose level cannot reach the selected database: `Datastore::link_columns`
//! and `Datastore::read_tables` decide whether the tables are read, and `read_tables` whether the
//! session may be told the columns.

use std::collections::BTreeSet;
use std::sync::{Mutex, PoisonError};

use anyhow::Result;
use surrealdb_sqlplan::{
	BinderCatalog, ColumnInfo, EmptyCatalog, MemoryCatalog, SqlType, TableSchema,
};

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{FieldDefinition, TableDefinition, TableType};
use crate::expr::{Kind, Part};
use crate::kvs::Transaction;
use crate::val::TableName;

/// A catalog that describes nothing, as [`EmptyCatalog`] does, and notes every
/// table the lowering asks it about.
#[derive(Default)]
pub(super) struct RecordingCatalog {
	tables: Mutex<BTreeSet<String>>,
	database: Option<String>,
}

impl RecordingCatalog {
	pub(super) fn with_database(database: Option<&str>) -> Self {
		Self {
			tables: Mutex::new(BTreeSet::new()),
			database: database.map(str::to_owned),
		}
	}

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
	fn current_database(&self) -> Option<&str> {
		self.database.as_deref()
	}

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
pub(super) fn link_catalog(
	links: &[(String, String, String)],
	database: Option<&str>,
) -> Option<MemoryCatalog> {
	if links.is_empty() {
		return None;
	}
	let mut catalog = MemoryCatalog::new();
	if let Some(db) = database {
		catalog.set_database(Some(db.to_owned()));
	}
	Some(
		links.iter().fold(catalog, |catalog, (table, column, target)| {
			catalog.with_link(table, column, target)
		}),
	)
}

/// The SQL type of a field of `kind`, where it has one wire representation. Optionality does not
/// change the type, and an array of such a scalar is an array of its type. Mixed unions, record
/// links, sets, nested arrays and arrays of anything else are [`SqlType::Unknown`], typed from the
/// values they hold.
///
/// [`SqlType::Unknown`]: surrealdb_sqlplan::SqlType::Unknown
fn scalar_type(kind: &Kind) -> SqlType {
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

/// Whether none of the PostgreSQL `stmts` can change the columns of a table, so that the tables
/// read before any of them runs are the tables each of them sees.
///
/// A statement is listed as keeping the columns; one that is not listed, whatever is added to the
/// dialect, is taken to change them.
#[cfg(feature = "postgres")]
pub(super) fn pgsql_keeps_columns(stmts: &[surrealdb_pgsql::ast::stmt::Statement]) -> bool {
	use surrealdb_pgsql::ast::stmt::Statement;
	stmts.iter().all(|stmt| {
		matches!(
			stmt,
			Statement::Copy(_)
				| Statement::Select(_)
				| Statement::Insert(_)
				| Statement::Update(_)
				| Statement::Delete(_)
				| Statement::Truncate(_)
				| Statement::CreateIndex(_)
				| Statement::Transaction(_)
				| Statement::Set(_)
				| Statement::Show(_)
				| Statement::Reset(_)
				| Statement::Explain(_)
				| Statement::Prepare(_)
				| Statement::Execute(_)
				| Statement::Deallocate(_)
				| Statement::Empty
		)
	})
}

/// [`pgsql_keeps_columns`] for MySQL statements.
#[cfg(feature = "mysql")]
pub(super) fn mysql_keeps_columns(stmts: &[surrealdb_mysql::ast::stmt::Statement]) -> bool {
	use surrealdb_mysql::ast::stmt::Statement;
	stmts.iter().all(|stmt| {
		matches!(
			stmt,
			Statement::Select(_)
				| Statement::Insert(_)
				| Statement::Update(_)
				| Statement::Delete(_)
				| Statement::Replace(_)
				| Statement::Truncate(_)
				| Statement::CreateIndex(_)
				| Statement::DropIndex(_)
				| Statement::CreateUser(_)
				| Statement::DropUser(_)
				| Statement::Grant(_)
				| Statement::Revoke(_)
				| Statement::LockTables(_)
				| Statement::UnlockTables(_)
				| Statement::Prepare(_)
				| Statement::Execute(_)
				| Statement::Deallocate(_)
				| Statement::Use(_)
				| Statement::Set(_)
				| Statement::Show(_)
				| Statement::Describe(_)
				| Statement::Explain(_)
				| Statement::Transaction(_)
		)
	})
}

/// What the datastore knows of the tables a statement names, as the catalog it is bound against.
pub(super) struct Described {
	pub(super) catalog: MemoryCatalog,
	/// Whether lowering against `catalog` can differ from lowering against none: a column holds
	/// links, or a table declares all the columns it has.
	pub(super) shapes_lowering: bool,
}

/// Why the tables of a statement are read.
#[derive(Clone, Copy)]
pub(super) enum Purpose {
	/// Lowering the statements of a request.
	Lowering,
	/// Describing the result of one `SELECT` to its client, which is also told the types of the
	/// columns it reads.
	Describing,
}

impl Purpose {
	/// Whether the scalar types of columns are read.
	pub(super) fn types_columns(self) -> bool {
		match self {
			Self::Lowering => false,
			Self::Describing => true,
		}
	}
}

/// The tables `tables` of the database `ns`/`db`, as read through `txn`.
///
/// A column that holds links is named. With `types` a column of a scalar type is typed, and with
/// `columns` a table that is `SCHEMAFULL` and not a view is described as having the columns it
/// defines and no others: see [`declared_columns`]. A database that does not exist has no tables.
///
/// `columns` is set only for a reader that may be told the columns, which is what viewing the
/// definitions of the tables allows. Without it only the links are read: they decide how an
/// operand is converted and are never answered to the reader, whereas a column that is declared
/// reads as NONE and one that is not is an error, which tells them apart.
///
/// The catalog names `db` as its current database, which is what the table-name resolution of both
/// dialects checks a qualified name against: a table of another database is refused, where a
/// catalog without a current database accepts any qualifier.
pub(super) async fn describe_tables(
	txn: &Transaction,
	ns: &str,
	db: &str,
	tables: &[String],
	columns: bool,
	types: bool,
) -> Result<Described> {
	let mut catalog = MemoryCatalog::new();
	catalog.set_database(Some(db.to_owned()));
	let mut described = Described {
		catalog,
		shapes_lowering: false,
	};
	let Some(database) = txn.get_db_by_name(ns, db, None).await? else {
		return Ok(described);
	};
	for table in tables {
		let name = TableName::from(table.as_str());
		let fields = txn.all_tb_fields(database.namespace_id, database.database_id, &name, None);
		let fields = fields.await?;
		for field in fields.iter() {
			if field.is_key_part() {
				continue;
			}
			let (Some(column), Some(kind)) = (field.name.field_name(), &field.field_kind) else {
				continue;
			};
			if let Some(target) = link_target(kind) {
				described.catalog.add_link(None, table, column, target);
				described.shapes_lowering = true;
			}
			if types {
				let ty = scalar_type(kind);
				if ty != SqlType::Unknown {
					described.catalog.add_column_type(table, column, ty);
				}
			}
		}
		if !columns {
			continue;
		}
		let definition = txn.get_tb(database.namespace_id, database.database_id, &name, None);
		if let Some(definition) = definition.await?
			&& definition.schemafull
			&& definition.view.is_none()
		{
			let declared = declared_columns(&definition, &fields);
			described.catalog.add_table(TableSchema::new(table, declared).as_closed());
			described.shapes_lowering = true;
		}
	}
	Ok(described)
}

/// The columns of the `SCHEMAFULL` table `definition`, whose field definitions are `fields`: `id`
/// first, then the others in name order, the catalog not keeping the order they were defined in.
///
/// A field defined by a path (`address.city`, `tags[*]`) is a column by its first name. The record
/// id components of a table whose `id` is composed are not columns, and the endpoints of a
/// relation table, `in` and `out`, are.
fn declared_columns(definition: &TableDefinition, fields: &[FieldDefinition]) -> Vec<ColumnInfo> {
	let mut names: Vec<&str> = fields
		.iter()
		.filter(|field| !field.is_key_part())
		.filter_map(|field| match field.name.first() {
			Some(Part::Field(name)) => Some(name.as_str()),
			Some(_) | None => None,
		})
		.collect();
	if matches!(definition.table_type, TableType::Relation(_)) {
		names.extend(["in", "out"]);
	}
	names.sort_unstable();
	names.dedup();
	let rest = names.into_iter().filter(|name| *name != "id");
	std::iter::once("id")
		.chain(rest)
		.enumerate()
		.map(|(ordinal, name)| ColumnInfo::new(name, ordinal))
		.collect()
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

	#[test]
	fn an_array_of_a_scalar_is_typed_and_other_collections_are_not() {
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

	#[cfg(feature = "postgres")]
	#[test]
	fn a_pgsql_request_keeps_the_columns_unless_it_changes_a_table() {
		let keeps = |sql: &str| {
			pgsql_keeps_columns(&surrealdb_pgsql::parse_statements(sql).expect("the text parses"))
		};
		assert!(keeps("SELECT a FROM t; INSERT INTO t (a) VALUES (1); UPDATE t SET a = 2"));
		assert!(keeps("DELETE FROM t; TRUNCATE t; CREATE INDEX i ON t (a); BEGIN; COMMIT"));
		assert!(!keeps("ALTER TABLE t ADD COLUMN b INT; SELECT b FROM t"));
		assert!(!keeps("SELECT a FROM t; DROP TABLE t"));
		assert!(!keeps("CREATE TABLE t (a INT)"));
		assert!(!keeps("CREATE VIEW v AS SELECT a FROM t"));
	}

	#[cfg(feature = "mysql")]
	#[test]
	fn a_mysql_request_keeps_the_columns_unless_it_changes_a_table() {
		let keeps = |sql: &str| {
			mysql_keeps_columns(&surrealdb_mysql::parse_statements(sql).expect("the text parses"))
		};
		assert!(keeps("SELECT a FROM t; INSERT INTO t (a) VALUES (1); UPDATE t SET a = 2"));
		assert!(keeps("REPLACE INTO t (a) VALUES (1); USE d; DELETE FROM t; TRUNCATE t"));
		assert!(!keeps("ALTER TABLE t ADD COLUMN b INT; SELECT b FROM t"));
		assert!(!keeps("SELECT a FROM t; DROP TABLE t"));
		assert!(!keeps("CREATE TABLE t (a INT)"));
		assert!(!keeps("DROP DATABASE d"));
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
