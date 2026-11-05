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
//!    the links, the vector columns, the HNSW indexes over them and the declared columns
//!    ([`describe_tables`]). If nothing they declare changes how a statement lowers, the first
//!    lowering already is the answer; otherwise the statements are lowered again against a catalog
//!    that describes the tables. A statement the first lowering refused is settled the same way: a
//!    refusal that rests on what the catalog holds (an `hnsw` index over a column whose dimensions
//!    are not known) is lifted by the second lowering, and one that does not is reported again.
//!
//! A statement over a system view that lists the user's tables (`information_schema.tables`,
//! `pg_class`, ...) names no table of the database, so the first lowering notes none. The recorder
//! notes that it was asked for such a view ([`RecordingCatalog::asked_table_listing`]), and the
//! statements are then lowered against a catalog that also lists the tables of the database
//! ([`database_tables`]), which `Datastore::listed_tables` reads for a session that may view them.
//!
//! A request that names no table (`SELECT 1`) reads nothing, and neither does one whose session the
//! datastore refuses or whose level cannot reach the selected database: `Datastore::link_columns`
//! and `Datastore::read_tables` decide whether the tables are read, and `read_tables` whether the
//! session may be told the columns.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};

use anyhow::Result;
use surrealdb_sqlplan::vector::VectorMetric;
use surrealdb_sqlplan::{
	BinderCatalog, ColumnInfo, ColumnShape, EmptyCatalog, ForeignKeyInfo, IndexInfo, MemoryCatalog,
	ReferentialAction, SqlType, TableSchema,
};

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{DefineDefault, FieldDefinition, IndexDefinition, TableDefinition, TableType};
use crate::expr::computed_deps::extract_computed_deps;
use crate::expr::reference::ReferenceDeleteStrategy;
use crate::expr::statements::define::SqlTypeDialect;
use crate::expr::statements::define::declared_type::DeclaredSqlType;
use crate::expr::{Expr, Function, Kind, Literal, Part};
use crate::kvs::Transaction;
use crate::val::TableName;

/// A catalog that describes nothing, as [`EmptyCatalog`] does, and notes every
/// table the lowering asks it about.
#[derive(Default)]
pub(super) struct RecordingCatalog {
	tables: Mutex<BTreeSet<String>>,
	database: Option<String>,
	listing: AtomicBool,
	deferred: AtomicBool,
	enums: AtomicBool,
}

impl RecordingCatalog {
	pub(super) fn with_database(database: Option<&str>) -> Self {
		Self {
			tables: Mutex::new(BTreeSet::new()),
			database: database.map(str::to_owned),
			listing: AtomicBool::new(false),
			deferred: AtomicBool::new(false),
			enums: AtomicBool::new(false),
		}
	}

	fn note(&self, table: &str) {
		self.tables.lock().unwrap_or_else(PoisonError::into_inner).insert(table.to_owned());
	}

	/// Whether the lowering asked for a system view whose rows list the user's tables.
	pub(super) fn asked_table_listing(&self) -> bool {
		self.listing.load(Ordering::Relaxed)
	}

	/// Whether the lowering asked for a user-defined enum, domain or composite type, which only the
	/// schema holds.
	pub(super) fn asked_user_types(&self) -> bool {
		self.enums.load(Ordering::Relaxed)
	}

	/// Whether the lowering left a refusal to the schema ([`BinderCatalog::defers_schema_checks`]),
	/// so that it must be lowered again against the schema even when the schema names no link.
	pub(super) fn deferred_schema_checks(&self) -> bool {
		self.deferred.load(Ordering::Relaxed)
	}

	/// Note `table` unless it is a system table, which the datastore does not hold.
	fn note_user_table(&self, schema: Option<&str>, table: &str) {
		if EmptyCatalog.get_table(schema, table).is_none() {
			self.note(table);
		}
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
		if matches!(name, "pg_type" | "pg_enum") {
			self.enums.store(true, Ordering::Relaxed);
		}
		if system.is_none() {
			self.note(name);
		} else if surrealdb_sqlplan::catalog::system::lists_user_tables(schema, name) {
			self.listing.store(true, Ordering::Relaxed);
		}
		system
	}

	fn enum_type(&self, _name: &str) -> Option<surrealdb_sqlplan::EnumType> {
		self.enums.store(true, Ordering::Relaxed);
		None
	}

	fn enum_types(&self) -> Vec<surrealdb_sqlplan::EnumType> {
		self.enums.store(true, Ordering::Relaxed);
		Vec::new()
	}

	fn domain_type(&self, _name: &str) -> Option<surrealdb_sqlplan::DomainType> {
		self.enums.store(true, Ordering::Relaxed);
		None
	}

	fn composite_type(&self, _name: &str) -> Option<surrealdb_sqlplan::CompositeType> {
		self.enums.store(true, Ordering::Relaxed);
		None
	}

	fn domain_types(&self) -> Vec<surrealdb_sqlplan::DomainType> {
		self.enums.store(true, Ordering::Relaxed);
		Vec::new()
	}

	fn composite_types(&self) -> Vec<surrealdb_sqlplan::CompositeType> {
		self.enums.store(true, Ordering::Relaxed);
		Vec::new()
	}

	fn link_table(&self, _schema: Option<&str>, table: &str, _column: &str) -> Option<String> {
		self.note(table);
		None
	}

	fn column_enum(
		&self,
		_schema: Option<&str>,
		table: &str,
		_column: &str,
	) -> Option<surrealdb_sqlplan::EnumColumn> {
		self.note(table);
		None
	}

	fn vector_dimension(&self, schema: Option<&str>, table: &str, _column: &str) -> Option<usize> {
		self.note_user_table(schema, table);
		None
	}

	fn write_type(&self, schema: Option<&str>, table: &str, _column: &str) -> SqlType {
		self.note_user_table(schema, table);
		SqlType::Unknown
	}

	fn null_columns(&self, schema: Option<&str>, table: &str) -> Vec<String> {
		self.note_user_table(schema, table);
		Vec::new()
	}

	fn default_columns(&self, schema: Option<&str>, table: &str) -> Vec<(String, Expr)> {
		self.note_user_table(schema, table);
		Vec::new()
	}

	fn hnsw_indexes(&self, schema: Option<&str>, table: &str) -> Vec<(String, VectorMetric)> {
		self.note_user_table(schema, table);
		Vec::new()
	}

	fn column_shape(
		&self,
		_schema: Option<&str>,
		table: &str,
		_column: &str,
	) -> Option<ColumnShape> {
		self.note(table);
		None
	}

	/// The lowering asks for the tables of the database to resolve a constraint name against them.
	fn constraint_tables(&self) -> Vec<TableSchema> {
		self.listing.store(true, Ordering::Relaxed);
		Vec::new()
	}

	fn defers_schema_checks(&self) -> bool {
		self.deferred.store(true, Ordering::Relaxed);
		true
	}
}

/// The types of the database `ns`/`db`, as the enum, domain and composite types PostgreSQL
/// knows them by. A database that does not exist has none.
pub(super) async fn user_types(
	txn: &Transaction,
	ns: &str,
	db: &str,
) -> Result<surrealdb_sqlplan::UserTypes> {
	use crate::catalog::providers::TypeProvider;
	let Some(database) = txn.get_db_by_name(ns, db, None).await? else {
		return Ok(Default::default());
	};
	let definitions = txn.all_db_types(database.namespace_id, database.database_id, None).await?;
	Ok(sql_user_types(&definitions))
}

/// The types of `definitions`, as the lowering describes them.
fn sql_user_types(definitions: &[crate::catalog::TypeDefinition]) -> surrealdb_sqlplan::UserTypes {
	let mut types = surrealdb_sqlplan::UserTypes::default();
	for definition in definitions {
		match sql_user_type(definition) {
			surrealdb_sqlplan::UserType::Enum(enumeration) => types.enums.push(enumeration),
			surrealdb_sqlplan::UserType::Domain(domain) => types.domains.push(domain),
			surrealdb_sqlplan::UserType::Composite(composite) => types.composites.push(composite),
		}
	}
	types
}

/// `definition` as the lowering describes it; a union keeps the order and identity of its stored
/// labels.
fn sql_user_type(definition: &crate::catalog::TypeDefinition) -> surrealdb_sqlplan::UserType {
	if let Some(labels) = definition.union() {
		return surrealdb_sqlplan::UserType::Enum(surrealdb_sqlplan::EnumType {
			name: definition.name.to_string(),
			id: definition.id.0,
			labels: labels
				.labels
				.iter()
				.map(|l| surrealdb_sqlplan::EnumTypeLabel {
					label: l.label.clone(),
					order: l.order,
					ident: l.ident,
				})
				.collect(),
		});
	}
	surrealdb_sqlplan::UserType::of_definition(
		definition.name.as_str(),
		definition.id.0,
		&definition.kind(),
		&definition.asserts,
		definition.default.as_ref(),
		definition.sql_type.as_deref(),
	)
}

/// The type of `definitions` the kind of `field` names, alone or with `none` and `null`, with
/// whether it is a domain (`true`) or a composite type (`false`). A union type is neither.
fn declared_user_type<'a>(
	field: &FieldDefinition,
	definitions: &'a [crate::catalog::TypeDefinition],
) -> Option<(&'a str, bool)> {
	let name = named_type(field.field_kind.as_ref()?)?;
	let definition = definitions.iter().find(|definition| definition.name.as_str() == name)?;
	if definition.union().is_some() {
		return None;
	}
	Some((definition.name.as_str(), definition.is_domain()))
}

/// Whether `name` is a domain of `definitions` declared `NOT NULL`, which a base whose kind admits
/// NULL states as an assertion.
fn domain_refuses_null(definitions: &[crate::catalog::TypeDefinition], name: &str) -> bool {
	definitions.iter().filter(|definition| definition.name.as_str() == name).any(|definition| {
		matches!(
			sql_user_type(definition),
			surrealdb_sqlplan::UserType::Domain(domain) if domain.not_null
		)
	})
}

/// The type `kind` names, alone or with `none` and `null`.
fn named_type(kind: &Kind) -> Option<&str> {
	if let Kind::Named(name) = kind {
		return Some(name.as_str());
	}
	let Kind::Either(kinds) = kind else {
		return None;
	};
	let mut rest = kinds.iter().filter(|kind| !matches!(kind, Kind::None | Kind::Null));
	if let (Some(Kind::Named(name)), None) = (rest.next(), rest.next()) {
		Some(name.as_str())
	} else {
		None
	}
}

/// `kind` with each type it names replaced by that type's body, as `definitions` define them, so
/// a column of a domain is described by the domain's base. A name `definitions` does not hold is
/// kept, and the replacement stops at a depth no definition reaches.
fn resolved_kind(kind: &Kind, definitions: &[crate::catalog::TypeDefinition]) -> Kind {
	fn resolve(kind: &Kind, definitions: &[crate::catalog::TypeDefinition], depth: u8) -> Kind {
		if depth == 0 || !kind.has_named_type() {
			return kind.clone();
		}
		if let Kind::Named(name) = kind {
			return match definitions.iter().find(|definition| definition.name.as_str() == name) {
				Some(definition) => resolve(&definition.kind(), definitions, depth - 1),
				None => kind.clone(),
			};
		}
		if let Kind::Either(kinds) = kind {
			return Kind::either(
				kinds.iter().map(|k| resolve(k, definitions, depth - 1)).collect(),
			);
		}
		if let Kind::Array(inner, len) = kind {
			return Kind::Array(Box::new(resolve(inner, definitions, depth - 1)), *len);
		}
		if let Kind::Set(inner, len) = kind {
			return Kind::Set(Box::new(resolve(inner, definitions, depth - 1)), *len);
		}
		kind.clone()
	}
	resolve(kind, definitions, 32)
}

/// Whether `name` is a union type of `definitions`.
fn names_union(definitions: &[crate::catalog::TypeDefinition], name: &str) -> bool {
	definitions
		.iter()
		.any(|definition| definition.name.as_str() == name && definition.union().is_some())
}

/// The table a field of this type links to; see [`surrealdb_sqlplan::link_target`].
fn link_target(kind: &Kind) -> Option<&str> {
	surrealdb_sqlplan::link_target(kind)
}

/// What the field definitions of some tables declare, for the lowering of SQL statements.
#[derive(Default)]
pub(super) struct Declared {
	/// The link columns, as `(table, column, target, admits NONE)`.
	pub(super) links: Vec<(String, String, String, bool)>,
	/// How the columns are declared, as `(table, column, shape)`, read only when a statement asks
	/// for them (`ALTER TABLE ... ADD FOREIGN KEY`).
	pub(super) shapes: Vec<(String, String, ColumnShape)>,
	/// The columns that hold the labels of an enum, as `(table, column, held)`.
	pub(super) enums: Vec<(String, String, surrealdb_sqlplan::EnumColumn)>,
	/// The columns whose declared SQL type is a JSON type, as `(table, column)`: text written to
	/// one is JSON text.
	pub(super) json: Vec<(String, String)>,
}

/// The link columns of `tables`, and their shapes when `shapes` asks, read through `txn` from
/// the database `ns`/`db`. A database that does not exist has none.
pub(super) async fn link_columns(
	txn: &Transaction,
	ns: &str,
	db: &str,
	tables: &[String],
	shapes: bool,
) -> Result<Declared> {
	let Some(database) = txn.get_db_by_name(ns, db, None).await? else {
		return Ok(Declared::default());
	};
	let types = {
		use crate::catalog::providers::TypeProvider;
		txn.all_db_types(database.namespace_id, database.database_id, None).await?
	};
	let is_enum = |name: &str| names_union(&types, name);
	let mut declared = Declared::default();
	for table in tables {
		let name = TableName::from(table.as_str());
		let fields = txn.all_tb_fields(database.namespace_id, database.database_id, &name, None);
		for field in fields.await?.iter() {
			if field.is_key_part() {
				continue;
			}
			let (Some(column), Some(named)) = (field.name.field_name(), &field.field_kind) else {
				continue;
			};
			let held = surrealdb_sqlplan::EnumColumn::of_kind(named, &is_enum);
			let resolved = resolved_kind(named, &types);
			let kind = &resolved;
			if let Some(target) = link_target(kind) {
				declared.links.push((
					table.clone(),
					column.to_owned(),
					target.to_owned(),
					surrealdb_sqlplan::kind_admits_none(kind),
				));
			}
			if let Some(held) = held {
				declared.enums.push((table.clone(), column.to_owned(), held));
			}
			if declared_json_type(field).is_some() {
				declared.json.push((table.clone(), column.to_owned()));
			}
			if shapes {
				let shape = ColumnShape::new(
					Some(kind.clone()),
					field.assert.as_ref(),
					!matches!(field.default, DefineDefault::None),
				);
				declared.shapes.push((table.clone(), column.to_owned(), shape));
			}
		}
	}
	Ok(declared)
}

/// A catalog naming what `declared` holds, as [`link_columns`] returns it, in the database
/// `database`, or `None` when it holds nothing.
pub(super) fn link_catalog(declared: &Declared, database: Option<&str>) -> Option<MemoryCatalog> {
	if declared.links.is_empty()
		&& declared.shapes.is_empty()
		&& declared.enums.is_empty()
		&& declared.json.is_empty()
	{
		return None;
	}
	let catalog = declared.links.iter().fold(
		MemoryCatalog::new(),
		|catalog, (table, column, target, admits_none)| {
			let catalog = catalog.with_link(table, column, target);
			if *admits_none {
				catalog.with_nullable_link(table, column)
			} else {
				catalog
			}
		},
	);
	let catalog = declared.enums.iter().fold(catalog, |catalog, (table, column, held)| {
		catalog.with_enum_column(table, column, held.clone())
	});
	let catalog = declared.shapes.iter().fold(catalog, |catalog, (table, column, shape)| {
		catalog.with_column_shape(table, column, shape.clone())
	});
	let mut catalog = declared.json.iter().fold(catalog, |catalog, (table, column)| {
		catalog.with_column_type(table, column, SqlType::Json)
	});
	if let Some(database) = database {
		catalog.set_database(Some(database.to_owned()));
	}
	Some(catalog)
}

/// The SQL type of a field of `kind`, where it has one wire representation. Optionality does not
/// change the type, and an array of such a scalar is an array of its type. An array of such an
/// array is a two-dimensional array of its type. Mixed unions, record links, sets, arrays nested
/// deeper than two and arrays of anything else are [`SqlType::Unknown`], typed from the values
/// they hold.
///
/// [`SqlType::Unknown`]: surrealdb_sqlplan::SqlType::Unknown
fn scalar_type(kind: &Kind) -> SqlType {
	match kind {
		Kind::Bool => SqlType::Bool,
		Kind::Bytes => SqlType::Bytea,
		Kind::Datetime => SqlType::TimestampTz,
		Kind::Date => SqlType::Date,
		Kind::Time => SqlType::Time,
		Kind::Timestamp => SqlType::Timestamp,
		Kind::Interval => SqlType::Interval,
		Kind::Decimal | Kind::BigDecimal => SqlType::Numeric,
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
		Kind::U64 => SqlType::UInt8,
		Kind::String => SqlType::Text,
		Kind::Uuid => SqlType::Uuid,
		Kind::Object => SqlType::Json,
		Kind::Enum(_) => SqlType::Unknown,
		Kind::Array(..) if vector_dimension(kind).is_some() => SqlType::Vector,
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
		| Kind::Named(_)
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

/// Whether a field of `kind` can hold `NONE` or `NULL`: its type is `any`, or is, or has a member
/// that is, `none` or `null`.
fn admits_absence(kind: &Kind) -> bool {
	match kind {
		Kind::Any | Kind::Named(_) | Kind::None | Kind::Null => true,
		Kind::Either(kinds) => kinds.iter().any(admits_absence),
		Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
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
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Array(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => false,
	}
}

/// The number of dimensions of a field of this type when it holds vectors: an array of floats of
/// a given length, which is the type `vector(n)` is stored as. Optionality does not change it.
fn vector_dimension(kind: &Kind) -> Option<usize> {
	match kind {
		Kind::Array(element, Some(count)) => {
			matches!(**element, Kind::Float | Kind::F32 | Kind::F16)
				.then(|| usize::try_from(*count).ok())
				.flatten()
		}
		Kind::Either(kinds) => {
			let mut found = None;
			for kind in kinds {
				if matches!(kind, Kind::None | Kind::Null) {
					continue;
				}
				if found.is_some() {
					return None;
				}
				found = Some(vector_dimension(kind)?);
			}
			found
		}
		Kind::Any
		| Kind::Named(_)
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
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
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Array(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => None,
	}
}

/// The PostgreSQL type name and nullability of a column declared as `kind`. A column with no
/// declared type, or one with no single wire type, has no type name, and is nullable.
fn column_type_name(kind: Option<&Kind>) -> (Option<String>, bool) {
	let Some(kind) = kind else {
		return (None, true);
	};
	(scalar_type(kind).name(), admits_absence(kind))
}

/// The wire type a field's declared SQL type names where its kind alone does not say it
/// ([`SqlType::of_declared`]). `None` for a field with no declared type, or one whose type follows
/// from its kind.
fn declared_wire_type(field: &FieldDefinition) -> Option<SqlType> {
	let declared = DeclaredSqlType::from_sql_type(field.sql_type.as_ref()?).ok()?;
	SqlType::of_declared(&declared)
}

/// The name of a PostgreSQL `json` or `jsonb` column's declared type, or of an array of either
/// (`jsonb[]`), which the column reports when its kind has no wire type of its own.
fn declared_json_name(field: &FieldDefinition) -> Option<String> {
	let declared = declared_sql_type(field)?;
	if declared.dialect() != SqlTypeDialect::PostgreSql {
		return None;
	}
	json_element(&declared)?;
	Some(format!("{}{}", declared.base(), "[]".repeat(declared.array_dimensions().len())))
}

/// The wire type of a PostgreSQL `json[]` or `jsonb[]` column of one or two dimensions, whose
/// kind, an array of `any`, does not say its elements are JSON documents. `None` for any other
/// field.
fn declared_json_array_type(field: &FieldDefinition) -> Option<SqlType> {
	let declared = declared_sql_type(field)?;
	if declared.dialect() != SqlTypeDialect::PostgreSql || declared.array_dimensions().is_empty() {
		return None;
	}
	json_element(&declared)?;
	declared.array_dimensions().iter().try_fold(SqlType::Json, |ty, _| SqlType::array_of(ty))
}

/// The JSON type the elements of `declared` are, at every array dimension it has, or the JSON
/// type it is when it has none.
fn json_element(
	declared: &DeclaredSqlType,
) -> Option<crate::expr::statements::define::declared_type::JsonFlavour> {
	let mut element = declared.clone();
	while let Some(inner) = element.element() {
		element = inner;
	}
	element.json_flavour()
}

/// The declared SQL type of a field declared as a JSON type of either dialect: PostgreSQL `json`
/// or `jsonb`, or MySQL `JSON`.
fn declared_json_type(field: &FieldDefinition) -> Option<DeclaredSqlType> {
	let declared = DeclaredSqlType::from_sql_type(field.sql_type.as_ref()?).ok()?;
	declared.json_flavour()?;
	Some(declared)
}

/// The SQL type a field is declared with (`SQL TYPE`), which the system views report for its
/// column. A declaration that no longer reads as a type of its dialect reports nothing, and the
/// column is reported by its kind.
fn declared_sql_type(field: &FieldDefinition) -> Option<DeclaredSqlType> {
	DeclaredSqlType::from_sql_type(field.sql_type.as_ref()?).ok()
}

/// The sequence that numbers a column whose `DEFAULT` draws its next value from one
/// (`sequence::nextval('name')`), as the SQL DDL defines it for a serial or auto-increment column.
fn default_sequence(field: &FieldDefinition) -> Option<String> {
	let DefineDefault::Set(Expr::FunctionCall(call)) = &field.default else {
		return None;
	};
	match (&call.receiver, call.arguments.as_slice()) {
		(Function::Normal(name), [Expr::Literal(Literal::String(sequence))])
			if name == "sequence::nextval" =>
		{
			Some(sequence.to_string())
		}
		_ => None,
	}
}

/// The SQL type name of the key of each table, by table name: the type of its `id` field, which a
/// column that links to the table reports as its own type. A table whose `id` declares no single
/// type has no entry value.
type KeyTypes = std::collections::HashMap<String, Option<String>>;

/// The columns [`database_tables`] lists for a table with these field definitions, `id` first.
///
/// A column is a top-level field: a nested path (`a.b`, `a[*]`) and the virtual definition of a
/// record id component are not columns. `id` is always the first column and never nullable; the
/// other columns follow in the order the definitions are given. A column that links to one table
/// has the type of that table's key (`keys`).
fn table_columns(
	fields: &[FieldDefinition],
	keys: &KeyTypes,
	user_types: &[crate::catalog::TypeDefinition],
) -> Vec<ColumnInfo> {
	let is_enum = |name: &str| names_union(user_types, name);
	let mut columns = Vec::with_capacity(fields.len() + 1);
	let id = fields.iter().find(|field| field.name.field_name() == Some("id"));
	let (data_type, _) = column_type_name(id.and_then(|field| field.field_kind.as_ref()));
	columns.push(ColumnInfo {
		name: "id".to_owned(),
		ordinal: 0,
		data_type,
		is_nullable: false,
		sequence: id.and_then(default_sequence),
		enum_held: None,
		domain: None,
		composite: None,
		declared: id.and_then(declared_sql_type),
	});
	for field in fields {
		let Some(name) = field.name.field_name() else {
			continue;
		};
		if field.is_key_part() || name == "id" {
			continue;
		}
		// A column of a domain is described by the domain's base.
		let resolved = field.field_kind.as_ref().map(|kind| resolved_kind(kind, user_types));
		let (mut data_type, mut is_nullable) = column_type_name(resolved.as_ref());
		// A PostgreSQL column whose declared type names its own wire type reports that type.
		if let Some(declared) = declared_wire_type(field)
			&& field.sql_type.as_ref().is_some_and(|ty| ty.dialect == SqlTypeDialect::PostgreSql)
		{
			data_type = declared.name();
		}
		// A PostgreSQL JSON column is `any`, and an array of them an array of `any`, which have no
		// wire type of their own, so each keeps the JSON type its declared SQL type names.
		if data_type.is_none()
			&& let Some(json) = declared_json_name(field)
		{
			data_type = Some(json);
		}
		// A `NOT NULL` assertion makes the column required whatever its type admits.
		if surrealdb_sqlplan::ColumnShape::new(None, field.assert.as_ref(), false).assert
			== surrealdb_sqlplan::ColumnAssert::NotNull
		{
			is_nullable = false;
		}
		// A link column is typed as the key of the table it links to.
		if data_type.is_none()
			&& let Some(target) = field.field_kind.as_ref().and_then(link_target)
		{
			data_type = keys.get(target).cloned().flatten();
		}
		columns.push(ColumnInfo {
			name: name.to_owned(),
			ordinal: columns.len(),
			data_type,
			is_nullable,
			sequence: default_sequence(field),
			enum_held: field
				.field_kind
				.as_ref()
				.and_then(|kind| surrealdb_sqlplan::EnumColumn::of_kind(kind, &is_enum)),
			domain: None,
			composite: None,
			declared: declared_sql_type(field),
		});
		if let (Some((name, is_domain)), Some(column)) =
			(declared_user_type(field, user_types), columns.last_mut())
		{
			if is_domain {
				column.domain = Some(name.to_owned());
				// A column of a `NOT NULL` domain refuses NULL whatever its own kind admits.
				if domain_refuses_null(user_types, name) {
					column.is_nullable = false;
				}
			} else {
				column.composite = Some(name.to_owned());
			}
		}
	}
	columns
}

/// The indexes [`database_tables`] lists for a table: the ordered and unique indexes over plain
/// top-level fields. An index being removed, one of another kind (full-text, vector, count) and
/// one over a computed or nested path have no PostgreSQL counterpart and are not listed.
fn table_indexes(definitions: &[IndexDefinition]) -> Vec<IndexInfo> {
	definitions
		.iter()
		.filter(|index| !index.prepare_remove)
		.filter_map(|index| {
			let unique = match index.index {
				crate::catalog::Index::Idx => false,
				crate::catalog::Index::Uniq => true,
				crate::catalog::Index::Hnsw(_)
				| crate::catalog::Index::FullText(_)
				| crate::catalog::Index::Count(_)
				| crate::catalog::Index::DiskAnn(_)
				| crate::catalog::Index::Spatial => return None,
			};
			let columns = index
				.cols
				.iter()
				.map(|col| col.field_name().map(str::to_owned))
				.collect::<Option<Vec<_>>>()?;
			Some(IndexInfo {
				name: index.name.as_str().to_owned(),
				columns,
				unique,
			})
		})
		.collect()
}

/// The foreign keys [`database_tables`] lists for a table: each top-level field declared
/// `REFERENCE` whose type is a link to one table ([`link_target`]).
fn table_foreign_keys(fields: &[FieldDefinition]) -> Vec<ForeignKeyInfo> {
	fields
		.iter()
		.filter(|field| !field.is_key_part())
		.filter_map(|field| {
			let reference = field.reference.as_ref()?;
			let column = field.name.field_name()?;
			let target = link_target(field.field_kind.as_ref()?)?;
			let on_delete = match reference.on_delete {
				ReferenceDeleteStrategy::Cascade => ReferentialAction::Cascade,
				ReferenceDeleteStrategy::Unset => ReferentialAction::SetNull,
				ReferenceDeleteStrategy::Reject
				| ReferenceDeleteStrategy::Ignore
				| ReferenceDeleteStrategy::Custom(_) => ReferentialAction::NoAction,
			};
			Some(ForeignKeyInfo {
				column: column.to_owned(),
				target_table: target.to_owned(),
				on_delete,
			})
		})
		.collect()
}

/// Every table of the database `ns`/`db` with its columns, in the `public` schema, read through
/// `txn`. A database that does not exist has none.
///
/// Normal tables, relation tables and views are all listed as tables: [`TableSchema`] carries no
/// kind. A table with no field definitions lists the column `id` alone. The columns follow
/// [`table_columns`], and the types are those [`scalar_type`] derives, named as `SqlType::name`
/// names them.
///
/// This reads every table definition and every field definition of the database, with the
/// datastore's own authority, and returns them all: the caller decides whether the session may see
/// them (`Datastore::listed_tables`).
pub(super) async fn database_tables(
	txn: &Transaction,
	ns: &str,
	db: &str,
) -> Result<Vec<TableSchema>> {
	let Some(database) = txn.get_db_by_name(ns, db, None).await? else {
		return Ok(Vec::new());
	};
	use crate::catalog::providers::TypeProvider;
	let tables = txn.all_tb(database.namespace_id, database.database_id, None).await?;
	let ns_id = database.namespace_id;
	let db_id = database.database_id;
	let user_types = txn.all_db_types(ns_id, db_id, None).await?;
	let definitions = futures::future::try_join_all(tables.iter().map(|table| async move {
		let (fields, indexes) = futures::future::try_join(
			txn.all_tb_fields(ns_id, db_id, &table.name, None),
			txn.all_tb_indexes(ns_id, db_id, &table.name, None),
		)
		.await?;
		let view_over: Vec<String> = table
			.view
			.as_ref()
			.map(|view| {
				view.source_tables().iter().map(|source| source.as_str().to_owned()).collect()
			})
			.unwrap_or_default();
		anyhow::Ok((table.name.as_str(), fields, indexes, view_over))
	}))
	.await?;
	let keys: KeyTypes = definitions
		.iter()
		.map(|(name, fields, ..)| {
			let id = fields.iter().find(|field| field.name.field_name() == Some("id"));
			let (data_type, _) = column_type_name(id.and_then(|field| field.field_kind.as_ref()));
			((*name).to_owned(), data_type)
		})
		.collect();
	Ok(definitions
		.iter()
		.map(|(name, fields, indexes, view_over)| {
			TableSchema::new(*name, table_columns(fields, &keys, &user_types))
				.with_schema("public")
				.with_indexes(table_indexes(indexes))
				.with_foreign_keys(table_foreign_keys(fields))
				.with_view_over(view_over.clone())
		})
		.collect())
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
				| Statement::CreateExtension(_)
				| Statement::CreateType(_)
				| Statement::AlterType(_)
				| Statement::CreateDomain(_)
				| Statement::AlterDomain(_)
				| Statement::CreateCompositeType(_)
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
	/// links or vectors, a vector column has an HNSW index, a column has a type that text written
	/// to it is read as, or a table declares all the columns it has.
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

/// Which columns of a table are told their scalar type.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnTypes {
	/// None.
	None,
	/// The columns of a date, time, timestamp or interval type, which a comparison or an
	/// arithmetic operation over them converts its other operand to.
	Temporal,
	/// Every column of a scalar type.
	All,
}

impl Purpose {
	/// The columns whose scalar types are read.
	pub(super) fn column_types(self) -> ColumnTypes {
		match self {
			Self::Lowering => ColumnTypes::Temporal,
			Self::Describing => ColumnTypes::All,
		}
	}
}

/// The tables `tables` of the database `ns`/`db`, as read through `txn`.
///
/// A column that holds links is named. With `types` the columns it selects of a scalar type are
/// typed, and with `columns` a table that is `SCHEMAFULL` and not a view is described as having the
/// columns it defines and no others: see [`declared_columns`]. A database that does not exist has
/// no tables.
///
/// `columns` is set only for a reader that may be told the columns, which is what viewing the
/// definitions of the tables allows. Without it only the links are read: they decide how an
/// operand is converted and are never answered to the reader, whereas a column that is declared
/// reads as NONE and one that is not is an error, which tells them apart.
///
/// A column that holds vectors is named with its dimensions, and the HNSW indexes over a table
/// that has one are listed. Like the links they are read with the datastore's authority and decide
/// how a statement lowers: how text written to the column is read, and whether a nearest-neighbour
/// search is asked of an index. A statement that defines an index over the column is told the
/// dimensions it needs, which only a reader who may define it learns from that statement's result.
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
	types: ColumnTypes,
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
	let user_types = {
		use crate::catalog::providers::TypeProvider;
		txn.all_db_types(database.namespace_id, database.database_id, None).await?
	};
	let is_enum = |name: &str| names_union(&user_types, name);
	let mut composite_columns = false;
	let mut targets: BTreeSet<String> = BTreeSet::new();
	for table in tables {
		let name = TableName::from(table.as_str());
		let fields = txn.all_tb_fields(database.namespace_id, database.database_id, &name, None);
		let fields = fields.await?;
		let mut has_vectors = false;
		for field in fields.iter() {
			if field.is_key_part() {
				continue;
			}
			let (Some(column), Some(declared)) = (field.name.field_name(), &field.field_kind)
			else {
				continue;
			};
			let held = surrealdb_sqlplan::EnumColumn::of_kind(declared, &is_enum);
			let resolved = resolved_kind(declared, &user_types);
			let kind = &resolved;
			if let Some(target) = link_target(kind) {
				targets.insert(target.to_owned());
				described.catalog.add_link(None, table, column, target);
				if surrealdb_sqlplan::kind_admits_none(kind) {
					described.catalog.add_nullable_link(table, column);
				}
				described.shapes_lowering = true;
			}
			if let Some(held) = held {
				described.catalog =
					std::mem::take(&mut described.catalog).with_enum_column(table, column, held);
				described.shapes_lowering = true;
			}
			if types != ColumnTypes::All {
				add_write_type(&mut described, table, column, kind);
			}
			if let Some(dimensions) = vector_dimension(kind) {
				described.catalog.add_vector(table, column, dimensions);
				described.shapes_lowering = true;
				has_vectors = true;
			}
			// A column declared with a SQL type is reported as that type to a client told the
			// types of the columns it reads.
			if types == ColumnTypes::All
				&& let Some(declared) = declared_sql_type(field)
			{
				described.catalog.add_sql_type(table, column, declared);
			}
			// Text written to a JSON column is JSON text, which the lowering parses.
			if declared_json_type(field).is_some() {
				described.catalog.add_column_type(table, column, SqlType::Json);
				described.shapes_lowering = true;
				continue;
			}
			// A declared SQL type that names its own wire type decides how the lowering reads the
			// column (a range column answers `@>` and `<@` with an element) as well as how the
			// column is described.
			let declared = declared_wire_type(field);
			if types != ColumnTypes::None
				&& let Some(ty) = declared
			{
				described.catalog.add_column_type(table, column, ty);
				described.shapes_lowering = true;
			} else if types != ColumnTypes::None {
				// A `json[]` or `jsonb[]` column is an array of `any`, which its kind alone
				// types as nothing.
				let mut ty = declared_json_array_type(field).unwrap_or_else(|| scalar_type(kind));
				// A link column is typed as the key of the table it links to.
				if types == ColumnTypes::All
					&& ty == SqlType::Unknown
					&& let Some(target) = link_target(kind)
				{
					ty = key_type(txn, &database, target).await?;
				}
				if ty != SqlType::Unknown && (types == ColumnTypes::All || ty.is_temporal()) {
					described.catalog.add_column_type(table, column, ty);
				}
			}
		}
		// A table with no vector column has no search to ask an index for.
		let indexes = if has_vectors {
			txn.all_tb_indexes(database.namespace_id, database.database_id, &name, None).await?
		} else {
			Default::default()
		};
		for index in indexes.iter() {
			let (crate::catalog::Index::Hnsw(params), false, [column]) =
				(&index.index, index.prepare_remove, index.cols.as_slice())
			else {
				continue;
			};
			let metric = match params.distance {
				crate::catalog::Distance::Euclidean => VectorMetric::L2,
				crate::catalog::Distance::Cosine => VectorMetric::Cosine,
				crate::catalog::Distance::InnerProduct => VectorMetric::NegativeInnerProduct,
				crate::catalog::Distance::Manhattan => VectorMetric::L1,
				crate::catalog::Distance::Chebyshev
				| crate::catalog::Distance::Hamming
				| crate::catalog::Distance::Jaccard
				| crate::catalog::Distance::Minkowski(_)
				| crate::catalog::Distance::Pearson
				| crate::catalog::Distance::CosineNormalized => continue,
			};
			if let Some(column) = column.field_name() {
				described.catalog.add_hnsw_index(table, column, metric);
				described.shapes_lowering = true;
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
			let mut declared = declared_columns(&definition, &fields);
			for field in fields.iter().filter(|field| !field.is_key_part()) {
				let (Some(column), Some((name, false))) =
					(field.name.field_name(), declared_user_type(field, &user_types))
				else {
					continue;
				};
				if let Some(info) = declared.iter_mut().find(|info| info.name == column) {
					info.composite = Some(name.to_owned());
					composite_columns = true;
				}
			}
			described.catalog.add_table(TableSchema::new(table, declared).as_closed());
			described.catalog.add_null_columns(table, null_columns(&fields));
			described.catalog.add_default_columns(table, default_columns(&fields));
			described.shapes_lowering = true;
		}
	}
	// The composite type of a column describes how a result reads it.
	if composite_columns {
		let composites = sql_user_types(&user_types).composites;
		described.catalog = std::mem::take(&mut described.catalog).with_composites(composites);
	}
	if types != ColumnTypes::All {
		// Text written to a link column names a record of the table it links to by that table's
		// key, so the type of the key of every target is read too.
		for target in targets.iter().filter(|target| !tables.contains(target)) {
			let name = TableName::from(target.as_str());
			let fields =
				txn.all_tb_fields(database.namespace_id, database.database_id, &name, None);
			for field in fields.await?.iter() {
				if field.is_key_part() || field.name.field_name() != Some("id") {
					continue;
				}
				if let Some(kind) = &field.field_kind {
					add_write_type(&mut described, target, "id", kind);
				}
			}
		}
	}
	Ok(described)
}

/// Note that text written to `column` of `table` is read as the type of `kind`, if it has one.
fn add_write_type(described: &mut Described, table: &str, column: &str, kind: &Kind) {
	let ty = scalar_type(kind);
	if matches!(
		ty,
		SqlType::Int2
			| SqlType::Int4
			| SqlType::Int8
			| SqlType::Float4
			| SqlType::Float8
			| SqlType::Numeric
			| SqlType::Bool
			| SqlType::TimestampTz
			| SqlType::Uuid
			| SqlType::Json
	) {
		described.catalog.add_write_type(table, column, ty);
		described.shapes_lowering = true;
	}
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

/// The columns of a table with the field definitions `fields` that hold NULL in a row written
/// without them, in name order: a top-level field other than `id` that has no type, or whose
/// type admits NULL, and that has no `DEFAULT`, `VALUE` or `COMPUTED` clause. A field with one
/// of those clauses is given its value by the write, and one whose type refuses NULL
/// (`option<int>`, a link that admits `NONE`) cannot hold it.
fn null_columns(fields: &[FieldDefinition]) -> Vec<String> {
	let mut columns: Vec<String> = fields
		.iter()
		.filter(|field| !field.is_key_part())
		.filter(|field| {
			matches!(field.default, DefineDefault::None)
				&& field.value.is_none()
				&& field.computed.is_none()
				&& field.field_kind.as_ref().is_none_or(surrealdb_sqlplan::kind_admits_null)
		})
		.filter_map(|field| field.name.field_name())
		.filter(|name| *name != "id")
		.map(str::to_owned)
		.collect();
	columns.sort_unstable();
	columns.dedup();
	columns
}

/// The columns of a table with the field definitions `fields` that take a default in a row written
/// without them, in name order, with the expression that gives it: a top-level field other than
/// `id` with a `DEFAULT` clause and with neither a `VALUE` nor a `COMPUTED` one, which the write
/// computes itself. A default that reads another field of the row (or whose reads cannot be
/// told) is left out: it is evaluated where it is used, which is not the row it is a default of.
fn default_columns(fields: &[FieldDefinition]) -> Vec<(String, Expr)> {
	let mut columns: Vec<(String, Expr)> = fields
		.iter()
		.filter(|field| !field.is_key_part())
		.filter(|field| field.value.is_none() && field.computed.is_none())
		.filter_map(|field| {
			let (DefineDefault::Set(default) | DefineDefault::Always(default)) = &field.default
			else {
				return None;
			};
			let reads = extract_computed_deps(default);
			if !reads.is_complete || !reads.fields.is_empty() {
				return None;
			}
			let name = field.name.field_name().filter(|name| *name != "id")?;
			Some((name.to_owned(), default.clone()))
		})
		.collect();
	columns.sort_by(|a, b| a.0.cmp(&b.0));
	columns.dedup_by(|a, b| a.0 == b.0);
	columns
}

/// The SQL type of the key of `table`, which is the type of its `id` field; a column that links
/// to the table has that type. A table with no `id` of a single type has none.
async fn key_type(
	txn: &Transaction,
	database: &crate::catalog::DatabaseDefinition,
	table: &str,
) -> Result<SqlType> {
	let fields = txn
		.all_tb_fields(database.namespace_id, database.database_id, &TableName::from(table), None)
		.await?;
	let id = fields.iter().find(|field| field.name.field_name() == Some("id"));
	Ok(id.and_then(|field| field.field_kind.as_ref()).map_or(SqlType::Unknown, scalar_type))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::{Idiom, Part};

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
	fn an_array_of_floats_of_a_given_length_is_a_vector() {
		let floats = |count: Option<u64>| Kind::Array(Box::new(Kind::Float), count);
		assert_eq!(vector_dimension(&floats(Some(3))), Some(3));
		assert_eq!(vector_dimension(&Kind::Array(Box::new(Kind::F32), Some(8))), Some(8));
		assert_eq!(
			vector_dimension(&Kind::Either(vec![Kind::None, Kind::Null, floats(Some(3))])),
			Some(3)
		);
		assert_eq!(scalar_type(&floats(Some(3))), SqlType::Vector);
		assert_eq!(scalar_type(&Kind::Either(vec![Kind::None, floats(Some(3))])), SqlType::Vector);
	}

	#[test]
	fn other_arrays_are_not_vectors() {
		let array = |kind: Kind, count: Option<u64>| Kind::Array(Box::new(kind), count);
		assert_eq!(vector_dimension(&array(Kind::Float, None)), None);
		assert_eq!(vector_dimension(&array(Kind::Int, Some(3))), None);
		assert_eq!(vector_dimension(&array(array(Kind::Float, Some(3)), Some(3))), None);
		assert_eq!(vector_dimension(&Kind::Float), None);
		assert_eq!(
			vector_dimension(&Kind::Either(vec![array(Kind::Float, Some(3)), Kind::String])),
			None
		);
		assert_eq!(scalar_type(&array(Kind::Float, None)), SqlType::Array(&SqlType::Float8));
		assert_eq!(scalar_type(&array(Kind::Int, Some(3))), SqlType::Array(&SqlType::Int8));
	}

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
		assert_eq!(
			scalar_type(&array(array(Kind::String))),
			SqlType::Array(&SqlType::Array(&SqlType::Text))
		);
		assert_eq!(scalar_type(&array(array(array(Kind::String)))), SqlType::Unknown);
		assert_eq!(scalar_type(&array(array(Kind::Any))), SqlType::Unknown);
		assert_eq!(scalar_type(&array(Kind::Any)), SqlType::Unknown);
		assert_eq!(scalar_type(&array(record("dept"))), SqlType::Unknown);
		assert_eq!(scalar_type(&Kind::Set(Box::new(Kind::String), None)), SqlType::Unknown);
	}

	fn array(kind: Kind) -> Kind {
		Kind::Array(Box::new(kind), None)
	}

	fn column(kind: &Kind) -> (Option<String>, bool) {
		column_type_name(Some(kind))
	}

	fn typed(name: &str, kind: Option<Kind>) -> FieldDefinition {
		FieldDefinition {
			name: Idiom::field(name),
			field_kind: kind,
			..Default::default()
		}
	}

	fn nested(path: &[&str]) -> Idiom {
		Idiom(path.iter().map(|name| Part::Field((*name).into())).collect())
	}

	#[test]
	fn a_column_is_named_by_its_scalar_type_and_nullable_unless_it_excludes_absence() {
		let named = |name: &str, nullable: bool| (Some(name.to_owned()), nullable);
		assert_eq!(column(&Kind::Int), named("bigint", false));
		assert_eq!(column(&Kind::String), named("text", false));
		assert_eq!(column(&Kind::Bool), named("boolean", false));
		assert_eq!(column(&Kind::Float), named("double precision", false));
		assert_eq!(column(&Kind::Decimal), named("numeric", false));
		assert_eq!(column(&Kind::Datetime), named("timestamp with time zone", false));
		assert_eq!(column(&Kind::Duration), named("interval", false));
		assert_eq!(column(&Kind::Uuid), named("uuid", false));
		assert_eq!(column(&Kind::Bytes), named("bytea", false));
		assert_eq!(column(&Kind::Object), named("jsonb", false));
		assert_eq!(column(&array(Kind::String)), named("text[]", false));
		assert_eq!(column(&array(array(Kind::Int))), named("bigint[]", false));
		assert_eq!(
			column(&Kind::Either(vec![Kind::None, array(Kind::Int)])),
			named("bigint[]", true)
		);
		assert_eq!(column(&Kind::Either(vec![Kind::String, Kind::Null])), named("text", true));
		assert_eq!(column(&Kind::Any), (None, true));
		assert_eq!(column(&Kind::Either(vec![Kind::Int, Kind::String])), (None, false));
		assert_eq!(column(&record("dept")), (None, false));
		assert_eq!(column_type_name(None), (None, true));
	}

	#[test]
	fn the_columns_of_a_table_are_its_top_level_fields_with_id_first() {
		let fields = [
			typed("name", Some(Kind::String)),
			FieldDefinition {
				name: nested(&["address", "city"]),
				field_kind: Some(Kind::String),
				..Default::default()
			},
			FieldDefinition {
				name: Idiom(vec![Part::Field("tags".into()), Part::All]),
				field_kind: Some(Kind::String),
				..Default::default()
			},
			typed("id", Some(Kind::Int)),
			typed("tags", Some(Kind::Either(vec![Kind::None, array(Kind::String)]))),
			FieldDefinition {
				key_part: Some(0),
				..typed("part", Some(Kind::Int))
			},
		];
		let column = |name: &str, ordinal, ty: Option<&str>, nullable| ColumnInfo {
			name: name.to_owned(),
			ordinal,
			data_type: ty.map(str::to_owned),
			is_nullable: nullable,
			sequence: None,
			enum_held: None,
			domain: None,
			composite: None,
			declared: None,
		};
		assert_eq!(
			table_columns(&fields, &KeyTypes::new(), &[]),
			vec![
				column("id", 0, Some("bigint"), false),
				column("name", 1, Some("text"), false),
				column("tags", 2, Some("text[]"), true),
			]
		);
		assert_eq!(table_columns(&[], &KeyTypes::new(), &[]), vec![column("id", 0, None, false)]);
	}

	#[test]
	fn a_link_column_has_the_type_of_the_key_it_links_to() {
		let fields = [
			typed("author", Some(record("user"))),
			typed("editor", Some(Kind::option(record("user")))),
			typed("owner", Some(record("team"))),
			typed("anyone", Some(Kind::Record(Vec::new()))),
		];
		let keys: KeyTypes = [("user".to_owned(), Some("bigint".to_owned()))].into();
		let columns = table_columns(&fields, &keys, &[]);
		let typed = |name: &str| {
			let column = columns.iter().find(|c| c.name == name).unwrap();
			(column.data_type.clone(), column.is_nullable)
		};
		assert_eq!(typed("author"), (Some("bigint".to_owned()), false));
		assert_eq!(typed("editor"), (Some("bigint".to_owned()), true));
		assert_eq!(typed("owner"), (None, false));
		assert_eq!(typed("anyone"), (None, false));
	}

	#[test]
	fn a_json_array_column_is_typed_by_its_declared_type() {
		let declared = |name: &str, kind: Kind, text: &str| FieldDefinition {
			sql_type: Some(crate::expr::statements::define::SqlType {
				dialect: SqlTypeDialect::PostgreSql,
				text: text.to_owned(),
			}),
			..typed(name, Some(kind))
		};
		let fields = [
			declared("j", Kind::option(array(Kind::Any)), "json[]"),
			declared("jb", array(Kind::Any), "jsonb[]"),
			declared("grid", array(array(Kind::Any)), "jsonb[][]"),
			declared("deep", array(array(array(Kind::Any))), "jsonb[][][]"),
			declared("doc", Kind::Any, "jsonb"),
		];
		let columns = table_columns(&fields, &KeyTypes::new(), &[]);
		let named = |name: &str| {
			let column = columns.iter().find(|c| c.name == name).unwrap();
			(column.data_type.clone(), column.is_nullable)
		};
		assert_eq!(named("j"), (Some("json[]".to_owned()), true));
		assert_eq!(named("jb"), (Some("jsonb[]".to_owned()), false));
		assert_eq!(named("grid"), (Some("jsonb[][]".to_owned()), false));
		assert_eq!(named("doc"), (Some("jsonb".to_owned()), true));
		let json_array = SqlType::Array(&SqlType::Json);
		assert_eq!(declared_json_array_type(&fields[0]), Some(json_array));
		assert_eq!(declared_json_array_type(&fields[1]), Some(json_array));
		assert_eq!(
			declared_json_array_type(&fields[2]),
			Some(SqlType::Array(&SqlType::Array(&SqlType::Json)))
		);
		assert_eq!(declared_json_array_type(&fields[3]), None);
		assert_eq!(declared_json_array_type(&fields[4]), None);
	}

	fn index(name: &str, cols: &[&str], kind: crate::catalog::Index) -> IndexDefinition {
		IndexDefinition {
			index_id: crate::catalog::IndexId(1),
			name: name.into(),
			table_name: TableName::from("t"),
			cols: cols.iter().map(|col| Idiom::field(*col)).collect(),
			index: kind,
			count_cond: None,
			prepare_remove: false,
			format_version: 1,
			comment: None,
			nulls_not_distinct: false,
		}
	}

	#[test]
	fn only_ordered_and_unique_indexes_over_plain_fields_are_listed() {
		use crate::catalog::Index;
		let removing = IndexDefinition {
			prepare_remove: true,
			..index("gone", &["a"], Index::Idx)
		};
		let nested = IndexDefinition {
			cols: vec![nested(&["a", "b"])],
			..index("deep", &["a"], Index::Idx)
		};
		let listed = table_indexes(&[
			index("t_a_idx", &["a"], Index::Idx),
			index("t_a_b_uniq", &["a", "b"], Index::Uniq),
			index("t_count", &[], Index::Count(None)),
			removing,
			nested,
		]);
		let info = |name: &str, columns: &[&str], unique| IndexInfo {
			name: name.to_owned(),
			columns: columns.iter().map(|c| (*c).to_owned()).collect(),
			unique,
		};
		assert_eq!(
			listed,
			vec![info("t_a_idx", &["a"], false), info("t_a_b_uniq", &["a", "b"], true)]
		);
	}

	#[test]
	fn a_reference_field_that_links_to_one_table_is_a_foreign_key() {
		let reference = |on_delete| {
			Some(crate::expr::reference::Reference {
				on_delete,
				materialize: None,
			})
		};
		let fields = [
			FieldDefinition {
				reference: reference(ReferenceDeleteStrategy::Cascade),
				..typed("author", Some(record("user")))
			},
			FieldDefinition {
				reference: reference(ReferenceDeleteStrategy::Reject),
				..typed("editor", Some(Kind::Either(vec![Kind::None, record("user")])))
			},
			FieldDefinition {
				reference: reference(ReferenceDeleteStrategy::Unset),
				..typed("many", Some(Kind::Record(vec!["a".into(), "b".into()])))
			},
			typed("plain", Some(record("user"))),
			FieldDefinition {
				reference: reference(ReferenceDeleteStrategy::Unset),
				..typed("owner", Some(record("org")))
			},
		];
		let key = |column: &str, target: &str, on_delete| ForeignKeyInfo {
			column: column.to_owned(),
			target_table: target.to_owned(),
			on_delete,
		};
		assert_eq!(
			table_foreign_keys(&fields),
			vec![
				key("author", "user", ReferentialAction::Cascade),
				key("editor", "user", ReferentialAction::NoAction),
				key("owner", "org", ReferentialAction::SetNull),
			]
		);
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
	fn the_recording_catalog_notes_when_a_table_listing_is_asked_for() {
		let catalog = RecordingCatalog::default();
		assert!(catalog.get_table(Some("information_schema"), "columns").is_some());
		assert!(catalog.asked_table_listing());
		let catalog = RecordingCatalog::default();
		assert!(catalog.get_table(None, "pg_catalog.pg_class").is_some());
		assert!(catalog.asked_table_listing());
		let catalog = RecordingCatalog::default();
		assert!(catalog.get_table(Some("pg_catalog"), "pg_type").is_some());
		assert!(catalog.asked_table_listing());
		let catalog = RecordingCatalog::default();
		assert!(catalog.get_table(Some("pg_catalog"), "pg_namespace").is_some());
		assert!(catalog.get_table(None, "emp").is_none());
		assert!(!catalog.asked_table_listing());
		assert_eq!(catalog.into_tables(), vec!["emp".to_owned()]);
	}

	#[test]
	fn the_recording_catalog_notes_tables_but_not_system_ones() {
		let catalog = RecordingCatalog::default();
		assert!(catalog.get_table(None, "emp").is_none());
		assert_eq!(catalog.link_table(None, "emp", "dept"), None);
		assert_eq!(catalog.link_table(None, "dept", "boss"), None);
		assert_eq!(catalog.vector_dimension(None, "doc", "embedding"), None);
		assert!(catalog.hnsw_indexes(None, "doc").is_empty());
		// A system table is not one the datastore holds, however it is asked about.
		assert_eq!(catalog.vector_dimension(None, "pg_type", "oid"), None);
		assert!(catalog.hnsw_indexes(None, "pg_class").is_empty());
		assert_eq!(
			catalog.into_tables(),
			vec!["dept".to_owned(), "doc".to_owned(), "emp".to_owned()]
		);
	}

	#[test]
	fn a_column_whose_default_draws_from_a_sequence_lists_it() {
		use crate::expr::FunctionCall;

		let drawing = |sequence: &str| {
			DefineDefault::Set(Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal("sequence::nextval".to_string()),
				arguments: vec![Expr::Literal(Literal::String(sequence.into()))],
			})))
		};
		let fields = vec![
			FieldDefinition {
				default: drawing("t_id_seq"),
				..typed("id", Some(Kind::Int))
			},
			FieldDefinition {
				default: drawing("t_n_seq"),
				..typed("n", Some(Kind::Int))
			},
			FieldDefinition {
				default: DefineDefault::Set(Expr::Literal(Literal::Integer(1))),
				..typed("plain", Some(Kind::Int))
			},
			typed("other", Some(Kind::String)),
		];
		let sequences: Vec<_> = table_columns(&fields, &KeyTypes::new(), &[])
			.into_iter()
			.map(|c| (c.name, c.sequence))
			.collect();
		assert_eq!(
			sequences,
			[
				("id".to_owned(), Some("t_id_seq".to_owned())),
				("n".to_owned(), Some("t_n_seq".to_owned())),
				("plain".to_owned(), None),
				("other".to_owned(), None),
			]
		);
	}

	#[test]
	fn a_column_that_admits_null_and_has_no_value_of_its_own_holds_null_when_left_out() {
		let nullable = |kind: Kind| Kind::Either(vec![Kind::None, Kind::Null, kind]);
		let one = DefineDefault::Set(Expr::Literal(Literal::Integer(1)));
		let fields = vec![
			typed("id", Some(nullable(Kind::Int))),
			typed("b", Some(nullable(Kind::String))),
			typed("a", Some(nullable(Kind::Int))),
			typed("untyped", Some(Kind::Any)),
			typed("required", Some(Kind::Int)),
			typed("optional", Some(Kind::Either(vec![Kind::None, Kind::Int]))),
			typed("link", Some(Kind::Either(vec![Kind::None, record("dept")]))),
			FieldDefinition {
				default: one,
				..typed("defaulted", Some(nullable(Kind::Int)))
			},
			FieldDefinition {
				default: DefineDefault::Always(Expr::Literal(Literal::Integer(1))),
				..typed("always", Some(nullable(Kind::Int)))
			},
			FieldDefinition {
				value: Some(Expr::Literal(Literal::Integer(1))),
				..typed("valued", Some(nullable(Kind::Int)))
			},
			FieldDefinition {
				computed: Some(Expr::Literal(Literal::Integer(1))),
				..typed("computed", Some(nullable(Kind::Int)))
			},
			FieldDefinition {
				name: nested(&["address", "city"]),
				field_kind: Some(nullable(Kind::String)),
				..Default::default()
			},
			typed("no_type", None),
		];
		assert_eq!(null_columns(&fields), ["a", "b", "no_type", "untyped"]);
	}
	#[test]
	fn a_column_with_a_default_and_no_value_of_its_own_takes_the_default_when_left_out() {
		let int = |n: i64| Expr::Literal(Literal::Integer(n));
		let fields = vec![
			FieldDefinition {
				default: DefineDefault::Set(int(1)),
				..typed("id", Some(Kind::Int))
			},
			FieldDefinition {
				default: DefineDefault::Set(int(7)),
				..typed("seven", Some(Kind::Int))
			},
			FieldDefinition {
				default: DefineDefault::Always(int(2)),
				..typed("always", Some(Kind::Int))
			},
			typed("plain", Some(Kind::Int)),
			FieldDefinition {
				default: DefineDefault::Set(int(3)),
				value: Some(int(4)),
				..typed("valued", Some(Kind::Int))
			},
			FieldDefinition {
				default: DefineDefault::Set(int(3)),
				computed: Some(int(4)),
				..typed("computed", Some(Kind::Int))
			},
			FieldDefinition {
				default: DefineDefault::Set(int(5)),
				name: nested(&["address", "city"]),
				field_kind: Some(Kind::Int),
				..Default::default()
			},
		];
		assert_eq!(
			default_columns(&fields),
			[("always".to_owned(), int(2)), ("seven".to_owned(), int(7))]
		);
		// No column is both given NULL and given a default when it is left out.
		let nulls = null_columns(&fields);
		assert!(default_columns(&fields).iter().all(|(column, _)| !nulls.contains(column)));
	}

	#[test]
	fn a_default_that_reads_another_field_is_not_listed_but_one_that_draws_a_number_is() {
		use crate::expr::{FunctionCall, Idiom};

		let draw = Expr::FunctionCall(Box::new(FunctionCall {
			receiver: Function::Normal("sequence::nextval".to_string()),
			arguments: vec![Expr::Literal(Literal::String("t_n_seq".into()))],
		}));
		let fields = vec![
			FieldDefinition {
				default: DefineDefault::Set(Expr::Idiom(Idiom::field("a".to_owned()))),
				..typed("reads", Some(Kind::Int))
			},
			FieldDefinition {
				default: DefineDefault::Set(draw.clone()),
				..typed("n", Some(Kind::Int))
			},
		];
		assert_eq!(default_columns(&fields), [("n".to_owned(), draw)]);
	}
}
