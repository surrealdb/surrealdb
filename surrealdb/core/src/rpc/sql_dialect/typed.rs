//! Column types, command tags and the planner analysis of one SQL statement.
//!
//! The analysis is what makes a result self-describing: the columns a `SELECT` answers with, in
//! projection order, each with the dialect's wire type, and the command tag a write or DDL
//! statement answers with.

#![cfg_attr(not(any(feature = "postgres", feature = "mysql")), allow(dead_code, unused_imports))]

use std::sync::Arc;

use super::{Options, error_entry, number};
use crate::dbs::Session;
use crate::kvs::{Datastore, Dialect, Transaction};
use crate::types::{PublicArray, PublicValue};

/// A value's type as the dialect's wire reports it.
#[derive(Clone)]
pub(in crate::rpc) struct TypeMeta {
	pub name: Option<String>,
	pub oid: i32,
	pub mysql_type: u8,
}

impl TypeMeta {
	const fn fixed(name: &'static str, oid: i32, mysql_type: u8) -> (&'static str, i32, u8) {
		(name, oid, mysql_type)
	}

	fn of(parts: (&'static str, i32, u8)) -> Self {
		Self {
			name: Some(parts.0.to_owned()),
			oid: parts.1,
			mysql_type: parts.2,
		}
	}

	/// The type of a column whose planner type is unknown, inferred from one of its values.
	fn infer(value: &PublicValue) -> Self {
		let parts = if let PublicValue::Bool(_) = value {
			Self::fixed("boolean", 16, 1)
		} else if let PublicValue::Number(n) = value {
			if matches!(
				n,
				surrealdb_types::Number::Float(_)
					| surrealdb_types::Number::F16(_)
					| surrealdb_types::Number::F32(_)
			) {
				Self::fixed("double precision", 701, 5)
			} else if matches!(n, surrealdb_types::Number::Decimal(_)) {
				Self::fixed("numeric", 1700, 246)
			} else {
				Self::fixed("bigint", 20, 8)
			}
		} else if let PublicValue::Bytes(_) = value {
			Self::fixed("bytea", 17, 252)
		} else if let PublicValue::Datetime(_) = value {
			Self::fixed("timestamp with time zone", 1184, 7)
		} else if let PublicValue::Duration(_) = value {
			Self::fixed("interval", 1186, 253)
		} else if let PublicValue::Uuid(_) = value {
			Self::fixed("uuid", 2950, 253)
		} else if matches!(
			value,
			PublicValue::Object(_) | PublicValue::Array(_) | PublicValue::Set(_)
		) {
			Self::fixed("json", 114, 245)
		} else {
			Self::fixed("text", 25, 253)
		};
		Self::of(parts)
	}

	#[cfg(any(feature = "postgres", feature = "mysql"))]
	fn from_sql(ty: surrealdb_sqlplan::SqlType) -> Self {
		Self {
			name: ty.name(),
			oid: ty.to_pg_oid(),
			mysql_type: ty.to_mysql_type(),
		}
	}

	/// The type as the dialect names it on the wire: `{ name, oid }` or `{ name, mysql_type }`.
	pub(super) fn value(&self, dialect: Dialect) -> PublicValue {
		let mut object = surrealdb_types::Object::new();
		object.insert("name", self.name.clone().map_or(PublicValue::Null, PublicValue::String));
		if dialect == Dialect::MySql {
			object.insert("mysql_type", number(i64::from(self.mysql_type)));
		} else {
			object.insert("oid", number(i64::from(self.oid)));
		}
		PublicValue::Object(object)
	}
}

/// One column of a result.
#[derive(Clone)]
pub(in crate::rpc) struct ColumnMeta {
	pub name: String,
	pub ty: TypeMeta,
	pub table: Option<String>,
	pub column: Option<String>,
	/// The field of each engine row that holds this column's value, when the planner keyed it.
	pub key: Option<String>,
}

impl ColumnMeta {
	/// A `text` column of the given name with no origin.
	pub(super) fn text(name: &str) -> Self {
		Self {
			name: name.to_owned(),
			ty: TypeMeta::of(TypeMeta::fixed("text", 25, 253)),
			table: None,
			column: None,
			key: None,
		}
	}

	pub(super) fn value(&self, dialect: Dialect) -> PublicValue {
		let mut object = surrealdb_types::Object::new();
		object.insert("name", PublicValue::String(self.name.clone()));
		object.insert("type", self.ty.value(dialect));
		object.insert("nullable", PublicValue::Null);
		object.insert("table", self.table.clone().map_or(PublicValue::Null, PublicValue::String));
		object.insert("column", self.column.clone().map_or(PublicValue::Null, PublicValue::String));
		PublicValue::Object(object)
	}
}

/// What a statement is, for its command tag and for whether it answers with rows.
#[derive(Clone)]
pub(in crate::rpc) enum Kind {
	Select,
	Insert,
	Update,
	Delete,
	/// MySQL `REPLACE`. PostgreSQL has no such statement, so only a build with the `mysql`
	/// feature has this variant.
	#[cfg(feature = "mysql")]
	Replace,
	/// Any other statement; its tag when it has one of its own, else a row count as a read has.
	Other(Option<&'static str>),
}

impl Kind {
	fn writes_rows(&self) -> bool {
		match self {
			Self::Insert | Self::Update | Self::Delete => true,
			#[cfg(feature = "mysql")]
			Self::Replace => true,
			Self::Select | Self::Other(_) => false,
		}
	}
}

/// The planner's analysis of one statement.
pub(in crate::rpc) struct Described {
	/// The columns in projection order, when the planner can list them before the statement runs.
	pub columns: Option<Vec<ColumnMeta>>,
	/// The type of each parameter.
	pub params: Vec<TypeMeta>,
	pub kind: Kind,
	/// Whether a write answers with the rows it wrote (a PostgreSQL `RETURNING` clause).
	pub returns_rows: bool,
	/// The `RETURNING` list in order: a column's name, or `None` for a `*` that stands for the
	/// columns the written row has. Empty when the statement has no such list.
	pub returning_order: Vec<Option<String>>,
	/// The `ERR` entry of a statement that does not plan.
	pub failure: Option<PublicValue>,
}

impl Described {
	fn plain(kind: Kind) -> Self {
		Self {
			columns: None,
			params: Vec::new(),
			kind,
			returns_rows: false,
			returning_order: Vec::new(),
			failure: None,
		}
	}
}

/// Analyse `text`, one statement, without running it. The dialect's capability is the caller's to
/// have checked. The catalog is read through the session and its open transaction, so a subject
/// is told only what it may read and sees its own uncommitted schema.
///
/// A statement kind with no tag of its own takes the generic one, so the match over the foreign
/// statement enums is deliberately open-ended.
pub(super) async fn analyse(
	ds: &Datastore,
	session: &Session,
	transaction: Option<&Arc<Transaction>>,
	dialect: Dialect,
	text: &str,
) -> Result<Described, surrealdb_types::Error> {
	let mut described = analyse_statement(ds, session, transaction, dialect, text).await?;
	if described.failure.is_none() && described.params.is_empty() {
		described.params = unknown_params(text, dialect);
	}
	Ok(described)
}

/// A parameter descriptor of unknown type for each positional parameter `text` names, for a
/// statement the planner does not type.
fn unknown_params(text: &str, dialect: Dialect) -> Vec<TypeMeta> {
	let mut count = 0usize;
	let mut quote: Option<char> = None;
	let mut chars = text.chars().peekable();
	while let Some(c) = chars.next() {
		if let Some(open) = quote {
			if c == open {
				quote = None;
			}
		} else if c == '\'' || c == '"' {
			quote = Some(c);
		} else if c == '/' && chars.next_if_eq(&'*').is_some() {
			// A block comment names no parameter.
			let mut previous = ' ';
			for inner in chars.by_ref() {
				if previous == '*' && inner == '/' {
					break;
				}
				previous = inner;
			}
		} else if (c == '-' && chars.next_if_eq(&'-').is_some())
			|| (dialect == Dialect::MySql && c == '#')
		{
			// A line comment runs to the end of the line.
			for inner in chars.by_ref() {
				if inner == '\n' {
					break;
				}
			}
		} else if dialect == Dialect::MySql && c == '?' {
			count += 1;
		} else if dialect != Dialect::MySql && c == '$' {
			let digits: String =
				std::iter::from_fn(|| chars.next_if(char::is_ascii_digit)).collect();
			if let Ok(n) = digits.parse::<usize>() {
				count = count.max(n);
			}
		}
	}
	(0..count.min(65_535))
		.map(|_| TypeMeta {
			name: None,
			oid: 0,
			mysql_type: 253,
		})
		.collect()
}

#[cfg_attr(not(any(feature = "postgres", feature = "mysql")), allow(unused_variables))]
#[allow(clippy::wildcard_enum_match_arm)]
async fn analyse_statement(
	ds: &Datastore,
	session: &Session,
	transaction: Option<&Arc<Transaction>>,
	dialect: Dialect,
	text: &str,
) -> Result<Described, surrealdb_types::Error> {
	match dialect {
		#[cfg(feature = "postgres")]
		Dialect::PgSql => {
			use surrealdb_pgsql::ast::stmt::Statement;
			use surrealdb_sqlplan::DialectPolicy;
			use surrealdb_sqlplan::binder::pgsql::bind_select;

			let statements = match crate::pgsql::parse(text) {
				Ok(statements) => statements,
				Err(err) => {
					let mut described = Described::plain(Kind::Other(None));
					described.failure = Some(error_entry(&err.to_string(), "42601", None));
					return Ok(described);
				}
			};
			let Some(statement) = statements.last() else {
				return Ok(Described::plain(Kind::Other(None)));
			};
			let mut described = match statement {
				Statement::Select(select) => {
					let catalog = ds.pgsql_catalog(session, transaction, select).await?;
					let policy = DialectPolicy::postgres();
					let mut described = Described::plain(Kind::Select);
					match bind_select(select, &catalog, &policy, None) {
						Ok(bound) => {
							described.params = bound
								.describe_parameters(&catalog)
								.param_types
								.into_iter()
								.map(TypeMeta::from_sql)
								.collect();
							described.columns =
								bound.describe_static(&catalog, &policy).map(columns_of);
						}
						Err(err) => {
							described.failure =
								Some(error_entry(&err.to_string(), err.pg_sqlstate(), None));
						}
					}
					described
				}
				Statement::Insert(insert) => {
					let mut described = Described::plain(Kind::Insert);
					described.returns_rows = insert.returning.is_some();
					described.returning_order = returning_order(insert.returning.as_deref());
					described
				}
				Statement::Update(update) => {
					let mut described = Described::plain(Kind::Update);
					described.returns_rows = update.returning.is_some();
					described.returning_order = returning_order(update.returning.as_deref());
					described
				}
				Statement::Delete(delete) => {
					let mut described = Described::plain(Kind::Delete);
					described.returns_rows = delete.returning.is_some();
					described.returning_order = returning_order(delete.returning.as_deref());
					described
				}
				Statement::Truncate(_) => Described::plain(Kind::Other(Some("TRUNCATE TABLE"))),
				Statement::CreateTable(_) => Described::plain(Kind::Other(Some("CREATE TABLE"))),
				Statement::CreateIndex(_) => Described::plain(Kind::Other(Some("CREATE INDEX"))),
				Statement::CreateView(_) => Described::plain(Kind::Other(Some("CREATE VIEW"))),
				Statement::CreateSchema(_) => Described::plain(Kind::Other(Some("CREATE SCHEMA"))),
				Statement::CreateExtension(_) => {
					Described::plain(Kind::Other(Some("CREATE EXTENSION")))
				}
				Statement::AlterTable(_) => Described::plain(Kind::Other(Some("ALTER TABLE"))),
				Statement::Drop(_) => Described::plain(Kind::Other(Some("DROP TABLE"))),
				_ => Described::plain(Kind::Other(None)),
			};
			if described.kind.writes_rows() && !described.returns_rows {
				described.columns = Some(Vec::new());
			}
			Ok(described)
		}
		#[cfg(feature = "mysql")]
		Dialect::MySql => {
			use surrealdb_mysql::ast::stmt::Statement;
			use surrealdb_sqlplan::DialectPolicy;
			use surrealdb_sqlplan::binder::mysql::bind_select;

			let statements = match crate::mysql::parse(text) {
				Ok(statements) => statements,
				Err(err) => {
					let mut described = Described::plain(Kind::Other(None));
					described.failure = Some(error_entry(&err.to_string(), "42000", Some(1064)));
					return Ok(described);
				}
			};
			let Some(statement) = statements.last() else {
				return Ok(Described::plain(Kind::Other(None)));
			};
			let described = match statement {
				Statement::Select(select) => {
					let catalog = ds.mysql_catalog(session, transaction, select).await?;
					let policy = DialectPolicy::mysql();
					let mut described = Described::plain(Kind::Select);
					match bind_select(select, &catalog, &policy, None) {
						Ok(bound) => {
							described.params = bound
								.describe_parameters(&catalog)
								.param_types
								.into_iter()
								.map(TypeMeta::from_sql)
								.collect();
							described.columns =
								bound.describe_static(&catalog, &policy).map(columns_of);
						}
						Err(err) => {
							described.failure = Some(error_entry(
								&err.to_string(),
								"42000",
								Some(i64::from(err.mysql_error_code())),
							));
						}
					}
					described
				}
				Statement::Insert(_) => Described::plain(Kind::Insert),
				Statement::Update(_) => Described::plain(Kind::Update),
				Statement::Delete(_) => Described::plain(Kind::Delete),
				Statement::Replace(_) => Described::plain(Kind::Replace),
				Statement::Truncate(_) => Described::plain(Kind::Other(Some("TRUNCATE"))),
				Statement::CreateTable(_) => Described::plain(Kind::Other(Some("CREATE TABLE"))),
				Statement::DropTable(_) => Described::plain(Kind::Other(Some("DROP TABLE"))),
				Statement::CreateIndex(_) => Described::plain(Kind::Other(Some("CREATE INDEX"))),
				Statement::AlterTable(_) => Described::plain(Kind::Other(Some("ALTER TABLE"))),
				_ => Described::plain(Kind::Other(None)),
			};
			let mut described = described;
			if described.kind.writes_rows() {
				described.columns = Some(Vec::new());
			}
			Ok(described)
		}
		_ => Err(super::invalid_params(
			"Describe is not available for this dialect in this build".to_string(),
		)),
	}
}

/// The order of a `RETURNING` list: each column's name, `None` for a `*`.
#[cfg(feature = "postgres")]
fn returning_order(
	targets: Option<&[surrealdb_pgsql::ast::stmt::ResTarget]>,
) -> Vec<Option<String>> {
	use surrealdb_pgsql::ast::expr::Expr;
	targets
		.unwrap_or_default()
		.iter()
		.map(|target| {
			let wildcard = matches!(&target.expr, Expr::ColumnRef(column)
				if column.fields.last().is_some_and(|name| name == "*"));
			(!wildcard).then(|| surrealdb_sqlplan::descriptor::pgsql::returning_name(target))
		})
		.collect()
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
fn columns_of(descriptor: surrealdb_sqlplan::ResultDescriptor) -> Vec<ColumnMeta> {
	descriptor
		.columns
		.into_iter()
		.map(|column| ColumnMeta {
			name: column.name,
			ty: TypeMeta::from_sql(column.ty),
			table: column.table,
			column: column.original_column,
			key: column.key,
		})
		.collect()
}

/// The command descriptor of a statement that answered with `count` rows.
pub(super) fn command(kind: &Kind, dialect: Dialect, count: usize) -> PublicValue {
	let mysql = dialect == Dialect::MySql;
	let tag = match kind {
		Kind::Select => {
			if mysql {
				"SELECT".to_owned()
			} else {
				format!("SELECT {count}")
			}
		}
		Kind::Insert => {
			if mysql {
				"INSERT".to_owned()
			} else {
				format!("INSERT 0 {count}")
			}
		}
		Kind::Update => {
			if mysql {
				"UPDATE".to_owned()
			} else {
				format!("UPDATE {count}")
			}
		}
		Kind::Delete => {
			if mysql {
				"DELETE".to_owned()
			} else {
				format!("DELETE {count}")
			}
		}
		#[cfg(feature = "mysql")]
		Kind::Replace => "REPLACE".to_owned(),
		Kind::Other(Some(tag)) => (*tag).to_owned(),
		Kind::Other(None) => {
			if mysql {
				"SELECT".to_owned()
			} else {
				format!("SELECT {count}")
			}
		}
	};
	let counted = !matches!(kind, Kind::Other(Some(_)));
	command_tag(&tag, counted.then_some(count))
}

/// A command descriptor with the given tag and affected-row count.
pub(super) fn command_tag(tag: &str, rows_affected: Option<usize>) -> PublicValue {
	let mut object = surrealdb_types::Object::new();
	object.insert("tag", PublicValue::String(tag.to_owned()));
	object.insert(
		"rows_affected",
		rows_affected.map_or(PublicValue::Null, |n| number(i64::try_from(n).unwrap_or(i64::MAX))),
	);
	object.insert("last_insert_id", PublicValue::Null);
	object.insert("warnings", number(0));
	PublicValue::Object(object)
}

/// `keys` arranged by a `RETURNING` list: the named columns where they are listed and every key the
/// list does not name at each `*`, in the engine's order. A named column is listed whether or not
/// the first row holds it (a `NONE` field is absent from the row). A list that does not account for
/// every key leaves the unaccounted keys last; without a list the keys keep the engine's order.
pub(super) fn ordered(keys: Vec<String>, order: &[Option<String>]) -> Vec<String> {
	if order.is_empty() {
		return keys;
	}
	let named: std::collections::HashSet<&str> =
		order.iter().flatten().map(String::as_str).collect();
	let mut out = Vec::with_capacity(keys.len());
	for entry in order {
		match entry {
			Some(name) => {
				if !out.contains(name) {
					out.push(name.clone());
				}
			}
			None => {
				out.extend(keys.iter().filter(|key| !named.contains(key.as_str())).cloned());
			}
		}
	}
	for key in keys {
		if !out.contains(&key) {
			out.push(key);
		}
	}
	out
}

/// The columns and rows of a result's engine rows. `columns` is the planner's projection when it
/// listed one; otherwise the columns are those of the first row, typed from their values.
pub(super) fn shape(
	rows: Vec<PublicValue>,
	columns: Option<&[ColumnMeta]>,
	order: &[Option<String>],
) -> (Vec<ColumnMeta>, Vec<Vec<PublicValue>>) {
	if let Some(columns) = columns {
		let shaped =
			rows.into_iter()
				.filter_map(|row| {
					if let PublicValue::Object(object) = row {
						let width = object.len();
						Some(
							columns
								.iter()
								.enumerate()
								.map(|(index, column)| {
									let found = match &column.key {
										Some(key) => object.get(key),
										None => object
											.get(column.name.as_str())
											.or_else(|| {
												column.column.as_ref().and_then(|original| {
													object.get(original.as_str())
												})
											})
											.or_else(|| {
												(width == columns.len())
													.then(|| object.values().nth(index))
													.flatten()
											}),
									};
									found.cloned().unwrap_or(PublicValue::Null)
								})
								.collect(),
						)
					} else if columns.len() == 1 {
						Some(vec![row])
					} else {
						None
					}
				})
				.collect();
		return (columns.to_vec(), shaped);
	}
	let names: Vec<String> = match rows.first() {
		Some(PublicValue::Object(first)) => ordered(first.keys().cloned().collect(), order),
		_ => Vec::new(),
	};
	let metas: Vec<ColumnMeta> = names
		.iter()
		.map(|name| {
			let sample = rows.iter().find_map(|row| {
				if let PublicValue::Object(object) = row {
					object
						.get(name.as_str())
						.filter(|value| !matches!(value, PublicValue::None | PublicValue::Null))
				} else {
					None
				}
			});
			ColumnMeta {
				name: name.clone(),
				ty: sample.map_or_else(
					|| TypeMeta::of(TypeMeta::fixed("text", 25, 253)),
					TypeMeta::infer,
				),
				table: None,
				column: None,
				key: None,
			}
		})
		.collect();
	let shaped = rows
		.into_iter()
		.filter_map(|row| {
			if let PublicValue::Object(object) = row {
				Some(
					names
						.iter()
						.map(|name| object.get(name.as_str()).cloned().unwrap_or(PublicValue::Null))
						.collect(),
				)
			} else {
				None
			}
		})
		.collect();
	(metas, shaped)
}

/// The `columns` and `rows` fields of an entry.
pub(super) fn columns_and_rows(
	columns: &[ColumnMeta],
	rows: Vec<Vec<PublicValue>>,
	dialect: Dialect,
	options: Options,
) -> (PublicValue, PublicValue) {
	let described = columns.iter().map(|column| column.value(dialect)).collect::<Vec<_>>();
	let rows = rows
		.into_iter()
		.map(|row| {
			let row: Vec<PublicValue> = if options.value_text {
				row.into_iter()
					.zip(columns)
					.map(|(value, column)| text_value(value, &column.ty, dialect))
					.collect()
			} else {
				row
			};
			if options.row_array {
				PublicValue::Array(row.into())
			} else {
				let mut object = surrealdb_types::Object::new();
				for (column, value) in columns.iter().zip(row) {
					object.insert(column.name.as_str(), value);
				}
				PublicValue::Object(object)
			}
		})
		.collect::<Vec<_>>();
	(PublicValue::Array(described.into()), PublicValue::Array(rows.into()))
}

/// The columns and parameters of a described statement as a `describe` answers with them.
pub(super) fn described_entry(
	described: &Described,
	dialect: Dialect,
) -> (PublicValue, PublicValue) {
	if let Some(failure) = &described.failure {
		return (failure.clone(), PublicValue::Array(PublicArray::new()));
	}
	let columns = match &described.columns {
		Some(columns) => PublicValue::Array(
			columns.iter().map(|column| column.value(dialect)).collect::<Vec<_>>().into(),
		),
		None => PublicValue::Null,
	};
	let mut entry = surrealdb_types::Object::new();
	entry.insert("status", PublicValue::String("OK".to_owned()));
	entry.insert("columns", columns);
	let params = described
		.params
		.iter()
		.map(|ty| {
			let mut object = surrealdb_types::Object::new();
			object.insert("type", ty.value(dialect));
			PublicValue::Object(object)
		})
		.collect::<Vec<_>>();
	(PublicValue::Object(entry), PublicValue::Array(params.into()))
}

/// Whether a statement of this kind answers with the rows it wrote, rather than a count.
pub(super) fn answers_with_rows(described: Option<&Described>) -> bool {
	match described {
		Some(described) => !described.kind.writes_rows() || described.returns_rows,
		None => true,
	}
}

/// The kind of a statement, `Select` when it was not analysed.
pub(super) fn kind_of(described: Option<&Described>) -> Kind {
	described.map_or(Kind::Select, |described| described.kind.clone())
}

/// A value as the dialect's text protocol writes it, `null` for SQL NULL. Booleans are `t` / `f`
/// for PostgreSQL and `1` / `0` for MySQL; bytes are `\x` hex for PostgreSQL; arrays of a
/// non-JSON type are PostgreSQL array literals; objects and JSON-typed values are JSON text.
pub(super) fn text_value(value: PublicValue, ty: &TypeMeta, dialect: Dialect) -> PublicValue {
	let mysql = dialect == Dialect::MySql;
	let json = ty.oid == 114;
	let text = if let PublicValue::None | PublicValue::Null = value {
		return PublicValue::Null;
	} else if let PublicValue::Bool(on) = value {
		match (on, mysql) {
			(true, false) => "t".to_owned(),
			(false, false) => "f".to_owned(),
			(true, true) => "1".to_owned(),
			(false, true) => "0".to_owned(),
		}
	} else if let PublicValue::Number(n) = value {
		number_text(n)
	} else if let PublicValue::String(s) = value {
		s
	} else if let PublicValue::Bytes(bytes) = value {
		let bytes = bytes.into_inner();
		if mysql {
			String::from_utf8_lossy(&bytes).into_owned()
		} else {
			let mut out = String::from("\\x");
			for byte in bytes.iter() {
				out.push_str(&format!("{byte:02x}"));
			}
			out
		}
	} else if let PublicValue::Datetime(datetime) = value {
		let iso = datetime.into_inner().to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);
		let spaced = iso.replace('T', " ");
		if mysql {
			spaced.trim_end_matches('Z').to_owned()
		} else {
			spaced.replace('Z', "+00")
		}
	} else if let PublicValue::Duration(duration) = value {
		let secs = duration.into_inner().as_secs();
		let (days, rest) = (secs / 86_400, secs % 86_400);
		let clock = format!("{:02}:{:02}:{:02}", rest / 3600, (rest % 3600) / 60, rest % 60);
		if days > 0 {
			format!("{days} days {clock}")
		} else {
			clock
		}
	} else if let PublicValue::Uuid(uuid) = value {
		uuid.into_inner().to_string()
	} else if let PublicValue::Array(items) = value {
		if json || mysql {
			PublicValue::Array(items).into_json_value().to_string()
		} else {
			pg_array_literal(items.into_vec())
		}
	} else {
		value.into_json_value().to_string()
	};
	PublicValue::String(text)
}

/// A number as the text protocol writes it: plain decimal digits with no SurrealQL width or kind
/// suffix, and `NaN`, `Infinity` and `-Infinity` for the non-finite floats.
fn number_text(number: surrealdb_types::Number) -> String {
	use surrealdb_types::Number;
	fn float<T: std::fmt::Display + Into<f64> + Copy>(value: T) -> String {
		let wide: f64 = value.into();
		if wide.is_nan() {
			"NaN".to_owned()
		} else if wide.is_infinite() {
			if wide < 0.0 {
				"-Infinity".to_owned()
			} else {
				"Infinity".to_owned()
			}
		} else {
			value.to_string()
		}
	}
	match number {
		Number::Int(v) => v.to_string(),
		Number::I8(v) => v.to_string(),
		Number::U8(v) => v.to_string(),
		Number::I16(v) => v.to_string(),
		Number::U16(v) => v.to_string(),
		Number::I32(v) => v.to_string(),
		Number::U32(v) => v.to_string(),
		Number::U64(v) => v.to_string(),
		Number::Float(v) => float(v),
		Number::F32(v) => float(v),
		Number::F16(v) => float(v.to_f32()),
		Number::Decimal(v) => v.to_string(),
		Number::BigDecimal(v) => v.to_string(),
	}
}

/// A PostgreSQL array literal of the text of `items`.
fn pg_array_literal(items: Vec<PublicValue>) -> String {
	let parts: Vec<String> = items
		.into_iter()
		.map(|item| {
			if let PublicValue::None | PublicValue::Null = item {
				return "NULL".to_owned();
			}
			let text = if let PublicValue::Array(nested) = item {
				return pg_array_literal(nested.into_vec());
			} else if let PublicValue::String(text) = item {
				text
			} else if let PublicValue::Bool(on) = item {
				return (if on {
					"t"
				} else {
					"f"
				})
				.to_owned();
			} else if let PublicValue::Number(n) = item {
				return number_text(n);
			} else {
				item.into_json_value().to_string()
			};
			let plain = !text.is_empty()
				&& !text.eq_ignore_ascii_case("null")
				&& text.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '-');
			if plain {
				text
			} else {
				format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
			}
		})
		.collect();
	format!("{{{}}}", parts.join(","))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn placeholders_in_comments_and_quotes_are_not_parameters() {
		let text = "INSERT INTO t (a) VALUES ($1) /* $2 */ -- $3\n; SELECT '$4'";
		assert_eq!(unknown_params(text, Dialect::PgSql).len(), 1);
		let text = "INSERT INTO t (a, b) VALUES (?, ?) /* ? */ # ?\n-- ?";
		assert_eq!(unknown_params(text, Dialect::MySql).len(), 2);
	}

	#[test]
	fn numbers_in_a_postgres_array_literal_are_plain_text() {
		use surrealdb_types::Number;
		let items = vec![
			PublicValue::Number(Number::I32(30)),
			PublicValue::Number(Number::U64(1)),
			PublicValue::Number(Number::F32(0.5)),
			PublicValue::Number(Number::Decimal("12.5".parse().unwrap())),
		];
		assert_eq!(pg_array_literal(items), "{30,1,0.5,12.5}");
	}

	#[test]
	fn numbers_are_plain_text_without_width_suffixes() {
		use surrealdb_types::Number;
		assert_eq!(number_text(Number::I32(30)), "30");
		assert_eq!(number_text(Number::U64(u64::MAX)), "18446744073709551615");
		assert_eq!(number_text(Number::Int(i64::MIN)), "-9223372036854775808");
		assert_eq!(number_text(Number::Float(1.5)), "1.5");
		assert_eq!(number_text(Number::F32(0.1)), "0.1");
		assert_eq!(number_text(Number::Float(f64::NEG_INFINITY)), "-Infinity");
		assert_eq!(number_text(Number::Float(f64::NAN)), "NaN");
		assert_eq!(number_text(Number::Decimal("12.50".parse().unwrap())), "12.50");
	}
}
