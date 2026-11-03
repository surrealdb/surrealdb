//! Typed DDL lowering from PostgreSQL and MySQL AST to SurrealDB Expr.
//!
//! Converts SQL DDL (`CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `ALTER TABLE`, `DROP`)
//! directly into SurrealDB `Expr::Define`, `Expr::Remove`, etc., without text serialization.

use std::collections::{BTreeSet, HashMap};

use surrealdb_expr::expr::field::Selector;
use surrealdb_expr::expr::index_kind::Index;
use surrealdb_expr::expr::kind::GeometryKind;
use surrealdb_expr::expr::operator::BinaryOperator;
use surrealdb_expr::expr::reference::{Reference, ReferenceDeleteStrategy};
use surrealdb_expr::expr::statements::alter::{AlterDefault, AlterKind};
use surrealdb_expr::expr::statements::define::{
	DefineDatabaseStatement, DefineDefault, DefineFieldStatement, DefineIndexStatement, DefineKind,
	DefineSequenceStatement, DefineStatement, DefineTableStatement, SqlType, SqlTypeDialect,
};
use surrealdb_expr::expr::statements::remove::{
	RemoveDatabaseStatement, RemoveFieldStatement, RemoveIndexStatement, RemoveSequenceStatement,
	RemoveStatement, RemoveTableStatement, SqlTypeObject,
};
use surrealdb_expr::expr::statements::{IfelseStatement, InfoStatement};
use surrealdb_expr::expr::table_type::TableType;
use surrealdb_expr::expr::visit::{MutVisitor, VisitMut};
use surrealdb_expr::expr::{
	Block, Cond, Expr, Field, Fields, Function, FunctionCall, Idiom, Kind, Literal, Param, Part,
	View,
};
use surrealdb_mysql::ast::stmt::{
	AlterTableAction as MyAlterTableAction, AlterTableStmt as MyAlterTableStmt,
	CreateDatabaseStmt as MyCreateDatabaseStmt, CreateIndexStmt as MyCreateIndexStmt,
	CreateTableStmt as MyCreateTableStmt, CreateViewStmt as MyCreateViewStmt,
	DropDatabaseStmt as MyDropDatabaseStmt, DropIndexStmt as MyDropIndexStmt,
	DropTableStmt as MyDropTableStmt, DropViewStmt as MyDropViewStmt, Statement as MyStatement,
	TableConstraint as MyTableConstraint, TableRef as MyTableRef,
};
use surrealdb_pgsql::ast::stmt::{
	AlterTableAction, AlterTableStmt, ColumnConstraint, CreateIndexStmt,
	CreateTableStmt as PgCreateTableStmt, CreateViewStmt, DropObjectType, DropStmt as PgDropStmt,
	Statement as PgStatement, TableConstraint as PgTableConstraint, TableRef as PgTableRef,
};
use surrealdb_strand::{Strand, TableName};

use super::foreign_key::{
	alter as alter_field, alter_column, check_create_targets, check_target_exists,
	column_foreign_key, drop_foreign_key, existence_assertion, link_kind, lower_add_foreign_key,
	reference, table_foreign_keys,
};
use super::write::{link_assignment, lower_mysql_expr, lower_pgsql_expr};
use crate::catalog::relations::{ConstraintTarget, constraint_target};
use crate::catalog::staged::StagedCatalog;
use crate::catalog::{BinderCatalog, DomainType, EmptyCatalog, TableSchema, kind_admits_none};
use crate::declared_type::DeclaredSqlType;
use crate::error::PlanError;

/// Whether the modifiers of a `float(p)` column request at most the 24 bits of mantissa that
/// PostgreSQL stores as `float4`. A `float` with no precision is `float8`.
fn has_single_precision(modifiers: &[String]) -> bool {
	modifiers.first().and_then(|p| p.trim().parse::<u32>().ok()).is_some_and(|p| p <= 24)
}

/// The most digits a `decimal` holds; a `numeric` of more is a `bigdecimal` (ADR 0014 D2).
const DECIMAL_DIGITS: u32 = 28;

/// The kind of a `numeric` or `decimal` column of `precision`: `decimal` up to 28 digits and
/// `bigdecimal` above, or with no precision, which in PostgreSQL is a `numeric` of any size.
pub(crate) fn decimal_kind(precision: Option<&str>) -> Kind {
	match precision.map(|p| p.trim().parse::<u32>()) {
		Some(Ok(p)) if p <= DECIMAL_DIGITS => Kind::Decimal,
		Some(Ok(_)) | Some(Err(_)) | None => Kind::BigDecimal,
	}
}

/// The declared SQL type of a PostgreSQL column: its type name, modifiers, and array dimensions,
/// in the canonical form [`DeclaredSqlType`] reads. A type the declaration cannot spell is refused.
pub(crate) fn pgsql_declared_type(
	type_name: &surrealdb_pgsql::ast::expr::TypeName,
) -> Result<SqlType, PlanError> {
	let name = type_name.name.parts.last().map_or("", String::as_str);
	let base = name.trim_end_matches("[]");
	let dimensions = type_name.array_bounds.len().max(name.matches("[]").count());
	let mut text = base.to_string();
	if !type_name.modifiers.is_empty() {
		text.push_str(&format!("({})", type_name.modifiers.join(",")));
	}
	for dimension in 0..dimensions {
		match type_name.array_bounds.get(dimension) {
			Some(Some(bound)) => text.push_str(&format!("[{bound}]")),
			Some(None) | None => text.push_str("[]"),
		}
	}
	declared_sql_type(SqlTypeDialect::PostgreSql, &text)
}

/// The declared SQL type of a PostgreSQL column of `type_name`: the base type of a domain, whose
/// values the column holds and whose text form, length and range the column checks, and otherwise
/// the type as written. The field's kind names the domain itself.
pub(crate) fn pgsql_column_declared_type(
	type_name: &surrealdb_pgsql::ast::expr::TypeName,
	catalog: &dyn BinderCatalog,
) -> Result<SqlType, PlanError> {
	match domain_of(type_name, catalog)? {
		Some(domain) => declared_sql_type(SqlTypeDialect::PostgreSql, &domain.base_type),
		None => pgsql_declared_type(type_name),
	}
}

/// The declared SQL type of a MySQL column, from the type as the column definition wrote it.
pub(crate) fn mysql_declared_type(type_str: &str) -> Result<SqlType, PlanError> {
	declared_sql_type(SqlTypeDialect::MySql, type_str)
}

/// Read `text` as a type of `dialect` and return its canonical form as the field's `SQL TYPE`.
fn declared_sql_type(dialect: SqlTypeDialect, text: &str) -> Result<SqlType, PlanError> {
	let declared = DeclaredSqlType::parse(dialect, text)
		.map_err(|err| PlanError::UnsupportedStatement(err.to_string()))?;
	Ok(SqlType {
		dialect,
		text: declared.canonical_text(),
	})
}

/// Map PostgreSQL TypeName to SurrealDB Kind. Integer and float widths follow PostgreSQL's
/// (`int2`/`int4`/`int8`, `float4`/`float8`) so a wire client sees the type it declared.
pub fn map_pgsql_type(type_name: &surrealdb_pgsql::ast::expr::TypeName) -> Kind {
	if super::vector_ddl::is_vector(type_name) {
		return super::vector_ddl::kind(type_name);
	}
	let raw_name = match type_name.name.parts.last() {
		Some(s) => s.to_ascii_lowercase(),
		None => return Kind::Any,
	};

	// Each dimension is one array level: `int[][]` is an array of arrays of `int`. A bound in the
	// brackets does not fix the length, as for one dimension.
	let dimensions = type_name.array_bounds.len().max(raw_name.matches("[]").count());
	let base_name = raw_name.trim_end_matches("[]");

	let inner_kind = match base_name {
		"int2" | "smallint" | "smallserial" | "serial2" => Kind::I16,
		"int" | "integer" | "int4" | "serial" | "serial4" => Kind::I32,
		"int8" | "bigint" | "bigserial" | "serial8" => Kind::Int,
		"float4" | "real" => Kind::F32,
		"float" if has_single_precision(&type_name.modifiers) => Kind::F32,
		"float" | "float8" | "double precision" | "double" => Kind::Float,
		"numeric" | "decimal" | "dec" => {
			decimal_kind(type_name.modifiers.first().map(String::as_str))
		}
		// `money` is a decimal with two fractional digits; the declared type carries the scale.
		"money" => Kind::Decimal,
		// The object identifier types and the transaction id of 32 bits.
		"oid" | "regclass" | "regproc" | "regprocedure" | "regoper" | "regoperator" | "regtype"
		| "regnamespace" | "regrole" | "regconfig" | "regdictionary" | "regcollation" | "xid"
		| "cid" => Kind::U32,
		// The 64-bit transaction id and the write-ahead log position.
		"xid8" | "pg_lsn" => Kind::U64,
		"text" | "varchar" | "char" | "character" | "character varying" | "bpchar" | "string"
		| "name" | "citext" | "inet" | "cidr" | "macaddr" | "macaddr8" | "bit" | "bit varying"
		| "varbit" | "xml" | "tsvector" | "tsquery" | "jsonpath" | "tid" | "pg_snapshot"
		| "txid_snapshot" => Kind::String,
		"bool" | "boolean" => Kind::Bool,
		"date" => Kind::Date,
		"time" | "timetz" | "time with time zone" | "time without time zone" => Kind::Time,
		"timestamp" | "timestamp without time zone" => Kind::Timestamp,
		"timestamptz" | "timestamp with time zone" => Kind::Datetime,
		// An interval with a field qualifier, such as `interval day to second`, is an interval;
		// its declared SQL type truncates a written value to the qualifier's fields.
		"interval" => Kind::Interval,
		name if name.strip_prefix("interval ").is_some_and(|fields| {
			surrealdb_pgsql::ast::expr::INTERVAL_FIELD_QUALIFIERS.contains(&fields)
		}) =>
		{
			Kind::Interval
		}
		"uuid" => Kind::Uuid,
		// A JSON document is any JSON value; its declared SQL type restricts a write to the
		// values JSON can carry.
		"json" | "jsonb" => Kind::Any,
		"bytea" => Kind::Bytes,
		// Geometric types (ADR 0014 D6). An open or closed `path` is a line or a polygon ring, so
		// its kind admits both; `line` and `circle` are objects of a fixed shape.
		"point" => Kind::Geometry(vec![GeometryKind::Point]),
		"lseg" => Kind::Geometry(vec![GeometryKind::Line]),
		"path" => Kind::Geometry(vec![GeometryKind::Line, GeometryKind::Polygon]),
		"polygon" | "box" => Kind::Geometry(vec![GeometryKind::Polygon]),
		"line" | "circle" => Kind::Object,
		// A range's element kind is in its declared SQL type (ADR 0014 D1, D7), so every range
		// shares one kind, and a multirange is an array of them.
		"int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
			Kind::Range
		}
		"int4multirange" | "int8multirange" | "nummultirange" | "tsmultirange"
		| "tstzmultirange" | "datemultirange" => Kind::Array(Box::new(Kind::Range), None),
		_ => Kind::Any,
	};

	(0..dimensions).fold(inner_kind, |kind, _| Kind::Array(Box::new(kind), None))
}

/// [`map_pgsql_type`], where a type name the mapping does not know is looked up as a user-defined
/// type in `catalog` (an enum, a domain or a composite type), and a column declared with one has
/// the kind that names it.
pub fn map_pgsql_type_in(
	type_name: &surrealdb_pgsql::ast::expr::TypeName,
	catalog: &dyn BinderCatalog,
) -> Kind {
	let kind = map_pgsql_type(type_name);
	// The innermost element, under every array dimension.
	let mut element = &kind;
	while let Kind::Array(inner, _) = element {
		element = inner;
	}
	if !matches!(element, Kind::Any) {
		return kind;
	}
	let Some(raw_name) = type_name.name.parts.last() else {
		return kind;
	};
	let name = raw_name.trim_end_matches("[]");
	let lowered = name.to_lowercase();
	if let Some(found) = catalog.enum_type(name).or_else(|| catalog.enum_type(&lowered)) {
		return with_element(&kind, Kind::Named(found.name));
	}
	if let Some(composite) =
		catalog.composite_type(name).or_else(|| catalog.composite_type(&lowered))
	{
		return with_element(&kind, Kind::Named(composite.name));
	}
	if let Some(domain) = catalog.domain_type(name).or_else(|| catalog.domain_type(&lowered)) {
		return with_element(&kind, Kind::Named(domain.name));
	}
	kind
}

/// `shape` with its innermost element replaced by `element`, keeping each array level and bound.
fn with_element(shape: &Kind, element: Kind) -> Kind {
	if let Kind::Array(inner, len) = shape {
		Kind::Array(Box::new(with_element(inner, element)), *len)
	} else {
		element
	}
}

/// The user-defined domain a column of `type_name` is declared with, if it is one. An array of a
/// domain is refused.
pub(crate) fn domain_of(
	type_name: &surrealdb_pgsql::ast::expr::TypeName,
	catalog: &dyn BinderCatalog,
) -> Result<Option<DomainType>, PlanError> {
	let mut element = map_pgsql_type(type_name);
	while let Kind::Array(inner, _) = element {
		element = *inner;
	}
	if !matches!(element, Kind::Any) {
		return Ok(None);
	}
	let Some(raw_name) = type_name.name.parts.last() else {
		return Ok(None);
	};
	let name = raw_name.trim_end_matches("[]");
	let lowered = name.to_lowercase();
	let Some(domain) = catalog.domain_type(name).or_else(|| catalog.domain_type(&lowered)) else {
		return Ok(None);
	};
	if !type_name.array_bounds.is_empty() || raw_name.ends_with("[]") {
		return Err(PlanError::unsupported(format!("an array of the domain \"{}\"", domain.name)));
	}
	Ok(Some(domain))
}

/// Lower the value a domain `CHECK` tests, which names the value under test as `VALUE`, to the
/// SurrealQL expression over `$value` that a column's `ASSERT` evaluates.
pub fn lower_pgsql_domain_check(
	check: &surrealdb_pgsql::ast::expr::Expr,
) -> Result<Expr, PlanError> {
	let mut lowered = lower_pgsql_expr(check)?;
	match lowered.visit_mut(&mut BindValue) {
		Ok(()) => Ok(lowered),
		Err(never) => match never {},
	}
}

/// Replaces each reference to the bare name `value` with the parameter `$value`.
struct BindValue;

impl MutVisitor for BindValue {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		if let Expr::Idiom(Idiom(parts)) = expr
			&& let [Part::Field(name)] = parts.as_slice()
			&& name.as_str().eq_ignore_ascii_case("value")
		{
			*expr = Expr::Param(Param::from("value".to_string()));
			return Ok(());
		}
		expr.visit_mut(self)
	}
}

/// Whether a MySQL `FLOAT(p)` column is stored in double precision: MySQL takes a lone precision
/// of 25 to 53 bits as `DOUBLE`. A `FLOAT` with no precision, a precision of at most 24, or the
/// `FLOAT(M,D)` form is single precision.
fn mysql_float_is_double(type_str: &str) -> bool {
	let Some((_, rest)) = type_str.split_once('(') else {
		return false;
	};
	let args = rest.split(')').next().unwrap_or("");
	!args.contains(',') && args.trim().parse::<u32>().is_ok_and(|p| p > 24)
}

/// The members of an `ENUM('a', 'b')` or `SET('a', 'b')` type text, in declaration order, with
/// a doubled quote read as one. `None` when the text holds no well-formed member list.
pub(crate) fn mysql_members(type_str: &str) -> Option<Vec<String>> {
	let open = type_str.find('(')?;
	let close = type_str.rfind(')')?;
	let mut chars = type_str.get(open + 1..close)?.chars().peekable();
	let mut members = Vec::new();
	loop {
		while chars.peek().is_some_and(|c| c.is_whitespace()) {
			chars.next();
		}
		if chars.next()? != '\'' {
			return None;
		}
		let mut member = String::new();
		loop {
			match chars.next()? {
				'\'' if chars.peek() == Some(&'\'') => {
					chars.next();
					member.push('\'');
				}
				'\'' => break,
				c => member.push(c),
			}
		}
		members.push(member);
		while chars.peek().is_some_and(|c| c.is_whitespace()) {
			chars.next();
		}
		match chars.next() {
			None => return Some(members),
			Some(',') => {}
			Some(_) => return None,
		}
	}
}

/// The most members a MySQL `SET` holds.
pub(crate) const MYSQL_SET_MEMBER_LIMIT: usize = 64;

/// Refuses a MySQL `SET` column of more than [`MYSQL_SET_MEMBER_LIMIT`] members.
pub(crate) fn check_mysql_set_limit(type_str: &str) -> Result<(), PlanError> {
	let lower = type_str.trim_start().to_ascii_lowercase();
	if lower.starts_with("set")
		&& !lower.starts_with("set_")
		&& let Some(members) = mysql_members(type_str)
		&& members.len() > MYSQL_SET_MEMBER_LIMIT
	{
		return Err(PlanError::unsupported(format!(
			"a SET column holds at most {MYSQL_SET_MEMBER_LIMIT} members, not {}",
			members.len()
		)));
	}
	Ok(())
}

/// Map MySQL data_type string to SurrealDB Kind. Integer widths follow MySQL's (`tinyint` 8 bits,
/// `smallint` 16, `mediumint` and `int` 32, `bigint` 64) and a trailing `UNSIGNED` or `ZEROFILL`
/// selects the unsigned kind of the same width. `BIGINT UNSIGNED` and `SERIAL` are `u64`, which
/// holds every value of the column they declare; the aliases name the same kinds as the types they
/// abbreviate.
pub fn map_mysql_type(type_str: &str) -> Kind {
	let lower = type_str.to_ascii_lowercase();
	let without_paren = lower.split('(').next().unwrap_or(&lower).trim();
	let first_word = without_paren.split_whitespace().next().unwrap_or("").trim();

	// A name of several words is not known by its first word alone.
	match without_paren {
		"double precision" => return Kind::Float,
		"national char"
		| "national character"
		| "national varchar"
		| "national char varying"
		| "national character varying"
		| "character varying"
		| "long"
		| "long varchar" => return Kind::String,
		"long varbinary" => return Kind::Bytes,
		_ => {}
	}

	// `ZEROFILL` implies `UNSIGNED`.
	let unsigned = lower.split_whitespace().any(|w| w == "unsigned" || w == "zerofill");

	match first_word {
		"tinyint" | "int1" if unsigned => Kind::U8,
		"tinyint" | "int1" => Kind::I8,
		"smallint" | "int2" if unsigned => Kind::U16,
		"smallint" | "int2" => Kind::I16,
		"mediumint" | "middleint" | "int3" | "int" | "integer" | "int4" if unsigned => Kind::U32,
		"mediumint" | "middleint" | "int3" | "int" | "integer" | "int4" => Kind::I32,
		"bigint" | "int8" if unsigned => Kind::U64,
		"bigint" | "int8" => Kind::Int,
		"serial" => Kind::U64,
		"float4" => Kind::F32,
		"float8" => Kind::Float,
		"float" if mysql_float_is_double(&lower) => Kind::Float,
		"float" => Kind::F32,
		"double" | "real" => Kind::Float,
		"decimal" | "dec" | "numeric" | "fixed" => {
			// MySQL `DECIMAL` without a precision is `DECIMAL(10,0)`.
			let precision = lower
				.split_once('(')
				.map(|(_, rest)| rest.split([',', ')']).next().unwrap_or("").trim());
			decimal_kind(Some(precision.unwrap_or("10")))
		}
		"enum" => match mysql_members(type_str) {
			Some(members) if !members.is_empty() => Kind::Enum(members),
			Some(_) | None => Kind::String,
		},
		"set" => match mysql_members(type_str) {
			Some(members) if !members.is_empty() => Kind::Set(Box::new(Kind::Enum(members)), None),
			Some(_) | None => Kind::String,
		},
		"char" | "nchar" | "varchar" | "nvarchar" | "tinytext" | "text" | "mediumtext"
		| "longtext" => Kind::String,
		"bool" | "boolean" => Kind::Bool,
		"date" => Kind::Date,
		// A MySQL `TIME` is a signed duration of up to 838:59:59, not a time of day.
		"time" => Kind::Interval,
		"datetime" => Kind::Timestamp,
		"timestamp" => Kind::Datetime,
		// A `YEAR` also takes text, which it reads differently from a number ('0' is 2000); the
		// declared type's validation stores either as the year it names.
		"year" => Kind::Either(vec![Kind::U16, Kind::String]),
		// A JSON document is any JSON value; its declared SQL type restricts a write to the
		// values JSON can carry.
		"json" => Kind::Any,
		"binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => Kind::Bytes,
		// `BIT(n)` for n in 1 to 64 is the unsigned integer of its bits.
		"bit" => Kind::U64,
		"vector" => super::vector_ddl::mysql_kind(&lower),
		// Spatial types (ADR 0014 D6). `GEOMETRY` admits every geometry kind.
		"geometry" => Kind::Geometry(Vec::new()),
		"point" => Kind::Geometry(vec![GeometryKind::Point]),
		"linestring" => Kind::Geometry(vec![GeometryKind::Line]),
		"polygon" => Kind::Geometry(vec![GeometryKind::Polygon]),
		"multipoint" => Kind::Geometry(vec![GeometryKind::MultiPoint]),
		"multilinestring" => Kind::Geometry(vec![GeometryKind::MultiLine]),
		"multipolygon" => Kind::Geometry(vec![GeometryKind::MultiPolygon]),
		"geometrycollection" | "geomcollection" => Kind::Geometry(vec![GeometryKind::Collection]),
		_ => Kind::Any,
	}
}

/// Whether a column of `kind` can hold `NONE`, where a domain admits it exactly when it is not
/// declared `NOT NULL`.
fn column_admits_none(kind: &Kind, catalog: &dyn BinderCatalog) -> bool {
	if let Kind::Either(kinds) = kind {
		kinds.iter().any(|kind| column_admits_none(kind, catalog))
	} else if let Kind::Named(name) = kind
		&& let Some(domain) = catalog.domain_type(name)
	{
		!domain.not_null
	} else {
		kind_admits_none(kind)
	}
}

/// Whether `kind` is a user-defined domain.
fn names_domain(kind: &Kind, catalog: &dyn BinderCatalog) -> bool {
	matches!(kind, Kind::Named(name) if catalog.domain_type(name).is_some())
}

/// The type of a column that may hold NULL unless a constraint forbids it, so
/// a column added to a table with rows accepts the rows that have no value.
pub(crate) fn column_kind(
	data_type: &surrealdb_pgsql::ast::expr::TypeName,
	not_null: bool,
	catalog: &dyn BinderCatalog,
) -> Kind {
	let base_kind = map_pgsql_type_in(data_type, catalog);
	// A domain says itself whether its column admits NULL.
	if not_null || names_domain(&base_kind, catalog) || matches!(base_kind, Kind::Any) {
		base_kind
	} else {
		Kind::either(vec![Kind::None, Kind::Null, base_kind])
	}
}

/// Whether a column of `kind` holds an object (`line`, `circle`, an array of them, or one that may
/// be NULL). Its field is `FLEXIBLE`, so an object keeps the keys it is written with in the
/// SCHEMAFULL table `CREATE TABLE` defines. A `json` or `jsonb` column, or an array of them, is not
/// `FLEXIBLE`, which only a kind holding an object may be: its kind holds `any` where a document
/// is, and the write path admits whatever a value of `any` holds.
fn holds_object(kind: &Kind) -> bool {
	match kind {
		Kind::Object => true,
		Kind::Array(elem, _) => holds_object(elem),
		Kind::Either(kinds) => kinds.iter().any(holds_object),
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
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => false,
	}
}

/// The kind of the MySQL column `name` mapped to `kind`. The `id` column is the record id key,
/// whose integers are signed 64-bit, so a `BIGINT UNSIGNED` or `SERIAL` key is an `int`; the
/// column's declared SQL type still names the unsigned type. Every other column keeps `kind`.
fn key_column_kind(name: &str, kind: Kind) -> Kind {
	if name == "id" && matches!(kind, Kind::U64) {
		Kind::Int
	} else {
		kind
	}
}

/// Whether a PostgreSQL column of `type_name` is a serial column, numbered by a sequence of its
/// own.
fn is_serial(type_name: &surrealdb_pgsql::ast::expr::TypeName) -> bool {
	type_name.array_bounds.is_empty()
		&& type_name.name.parts.last().is_some_and(|name| {
			matches!(
				name.to_ascii_lowercase().as_str(),
				"serial" | "serial4" | "bigserial" | "serial8" | "smallserial" | "serial2"
			)
		})
}

/// The name of the sequence that numbers the generated `column` of `table`: the table's name and
/// the column's name as SurrealQL spells a field (bare when it parses bare, quoted otherwise),
/// joined by a dot.
///
/// No two generated columns share a sequence: a quoted column name is escaped, so a dot or a
/// backtick inside it cannot read as the end of the table's name. Starting the sequence of a new
/// table over therefore never resets the numbering of another table's column. The column's part is
/// also how `INFO FOR TABLE` names the field, which is how a `TRUNCATE` finds the sequences of a
/// table again (`lower::ddl_truncate`).
pub(crate) fn sequence_name(table: &str, column: &str) -> String {
	format!("{table}.{}", surrealdb_types::ToSql::to_sql(&Idiom::field(column)))
}

/// `DEFINE SEQUENCE` of the generated-key sequence `name`, counting from 1 in batches of 1000.
pub(super) fn define_sequence(kind: DefineKind, name: Expr) -> Expr {
	Expr::Define(Box::new(DefineStatement::Sequence(DefineSequenceStatement {
		kind,
		name,
		batch: Expr::Literal(Literal::Integer(1000)),
		start: Expr::Literal(Literal::Integer(1)),
		..Default::default()
	})))
}

/// The sequence that numbers the serial or auto-increment `column` of `table`, starting at 1, and
/// the `DEFAULT` of the column that draws its next value. The `id` column's `DEFAULT` is its record
/// key when a write names no id.
///
/// The sequence is named by [`sequence_name`].
///
/// `if_not_exists` is set for a statement that may find its table already there (`CREATE TABLE IF
/// NOT EXISTS`): the sequence is defined only if it does not exist, so numbering under way carries
/// on, and [`restart_sequence_of_new_table`] starts it over when the statement creates the table.
/// Otherwise the statement creates the table, and the sequence is defined over one a dropped
/// table of the same name left, so the new table counts from 1 as in PostgreSQL and MySQL. That
/// relies on the table's definition coming first among the statement's expressions and on the
/// caller running them as one all-or-nothing block: a statement that finds its table there fails
/// on the table before the sequence is reached, so a live table's numbering is never reset.
fn column_sequence(table: &str, column: &str, if_not_exists: bool) -> (Expr, DefineDefault) {
	let name = sequence_name(table, column);
	let kind = if if_not_exists {
		DefineKind::IfNotExists
	} else {
		DefineKind::Overwrite
	};
	let name = Expr::Literal(Literal::String(Strand::from(name.as_str())));
	let next = Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal("sequence::nextval".to_string()),
		arguments: vec![name.clone()],
	}));
	(define_sequence(kind, name), DefineDefault::Set(next))
}

/// The expression that starts the sequence of the generated `column` of `table` over when `CREATE
/// TABLE IF NOT EXISTS` creates the table: `IF (INFO FOR DB).tables['table'] = NONE { DEFINE
/// SEQUENCE OVERWRITE ... }`. A sequence a dropped table of the same name left would otherwise be
/// kept, and the new table would carry on from its count.
///
/// It must run before the statement's definition of the table, which makes the answer true: it is
/// the first of the statement's expressions. A statement that finds its table there leaves the
/// sequence alone.
fn restart_sequence_of_new_table(table: &str, column: &str) -> Expr {
	let name = Expr::Literal(Literal::String(Strand::from(sequence_name(table, column).as_str())));
	let table_is_absent = Expr::Binary {
		left: Box::new(Expr::Idiom(Idiom(vec![
			Part::Start(Expr::Info(Box::new(InfoStatement::Db(false, None)))),
			Part::Field(Strand::from("tables")),
			Part::Value(Expr::Literal(Literal::String(Strand::from(table)))),
		]))),
		op: BinaryOperator::Equal,
		right: Box::new(Expr::Literal(Literal::None)),
	};
	Expr::IfElse(Box::new(IfelseStatement {
		exprs: vec![(
			table_is_absent,
			Expr::Block(Box::new(Block(vec![define_sequence(DefineKind::Overwrite, name)]))),
		)],
		close: None,
	}))
}

/// The assertion a `NOT NULL` column lowers to: its value is neither NONE nor NULL. The column's
/// type refuses NULL itself unless it names a type that admits NULL, such as a nullable domain.
pub(crate) fn not_null_assertion() -> Expr {
	Expr::Binary {
		left: Box::new(value_is_not(Literal::None)),
		op: BinaryOperator::And,
		right: Box::new(value_is_not(Literal::Null)),
	}
}

/// `$value != literal`.
fn value_is_not(literal: Literal) -> Expr {
	Expr::Binary {
		left: Box::new(Expr::Param(Param::from("value".to_string()))),
		op: BinaryOperator::NotEqual,
		right: Box::new(Expr::Literal(literal)),
	}
}

/// Whether `expr` is the assertion `NOT NULL` lowers to, or `$value != NONE` alone, which states
/// the same of a column whose type refuses NULL.
pub(crate) fn is_not_null_assertion(expr: &Expr) -> bool {
	*expr == not_null_assertion() || *expr == value_is_not(Literal::None)
}

/// Lower a PostgreSQL CREATE TABLE statement into SurrealDB expressions.
///
/// A column with a foreign key is a link to the referenced table; see [`super::foreign_key`].
pub fn lower_pgsql_create_table(create: &PgCreateTableStmt) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_create_table_with(create, &EmptyCatalog)
}

/// [`lower_pgsql_create_table`], where a column may be declared with a user-defined enum type
/// that `catalog` holds.
pub fn lower_pgsql_create_table_with(
	create: &PgCreateTableStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_name_str) = crate::namespace::resolve_pgsql_table_name(&create.name.parts, None)?;
	let tbl_name = tbl_name_str.as_str();
	let safe_idx_prefix = tbl_name.replace('.', "_");

	let mut exprs = Vec::new();
	let mut index_exprs = Vec::new();
	// What must run before the table is defined.
	let mut before = Vec::new();

	let table_def = DefineTableStatement {
		name: Expr::Table(TableName::from(tbl_name)),
		kind: if create.if_not_exists {
			DefineKind::IfNotExists
		} else {
			DefineKind::Default
		},
		full: true,
		table_type: TableType::Normal,
		..Default::default()
	};
	exprs.push(Expr::Define(Box::new(DefineStatement::Table(table_def))));

	let mut index_counter = 0;
	let foreign_keys = table_foreign_keys(create)?;

	for col in &create.columns {
		let is_id = col.name == "id";
		let is_table_pk = create.constraints.iter().any(|c| {
			if let PgTableConstraint::PrimaryKey(cols) = c {
				cols.iter().any(|c| c == &col.name)
			} else {
				false
			}
		});
		super::vector_ddl::check_column(&col.data_type)?;
		let serial = is_serial(&col.data_type);
		let domain = domain_of(&col.data_type, catalog)?;
		let own_not_null = is_id
			|| is_table_pk
			|| serial || col
			.constraints
			.iter()
			.any(|c| matches!(c, ColumnConstraint::NotNull | ColumnConstraint::PrimaryKey));
		let is_not_null = own_not_null;
		let foreign_key = foreign_keys.get(col.name.as_str());
		if foreign_key.is_some() && domain.is_some() {
			return Err(PlanError::unsupported(format!(
				"a foreign key on the column \"{}\" of \"{tbl_name}\", which is of a domain",
				col.name
			)));
		}
		if foreign_key.is_some() && (is_id || serial) {
			return Err(PlanError::UnsupportedStatement(format!(
				"a foreign key on the {} column \"{}\" of \"{tbl_name}\"",
				if is_id {
					"id"
				} else {
					"serial"
				},
				col.name
			)));
		}
		let field_kind = match foreign_key {
			Some((target, _)) => link_kind(target, !is_not_null),
			None => column_kind(&col.data_type, is_not_null, catalog),
		};

		let mut field_def = DefineFieldStatement {
			name: Expr::Idiom(Idiom::field(col.name.as_str())),
			what: Expr::Table(TableName::from(tbl_name)),
			flexible: holds_object(&field_kind),
			field_kind: Some(field_kind),
			kind: if create.if_not_exists {
				DefineKind::IfNotExists
			} else {
				DefineKind::Default
			},
			sql_type: Some(pgsql_column_declared_type(&col.data_type, catalog)?),
			..Default::default()
		};

		for con in &col.constraints {
			match con {
				ColumnConstraint::NotNull => {
					if foreign_key.is_none() {
						field_def.assert = Some(not_null_assertion());
					}
				}
				ColumnConstraint::Default(def) => {
					let lowered = lower_pgsql_expr(def)?;
					field_def.default = DefineDefault::Set(match foreign_key {
						Some((target, _)) => link_assignment(target, lowered, !is_not_null),
						None => lowered,
					});
				}
				ColumnConstraint::References {
					..
				} => {}
				ColumnConstraint::PrimaryKey => {
					index_counter += 1;
					let idx = DefineIndexStatement {
						name: Expr::Idiom(Idiom::field(format!(
							"{safe_idx_prefix}_{}_pk",
							col.name
						))),
						what: Expr::Table(TableName::from(tbl_name)),
						cols: vec![Expr::Idiom(Idiom(vec![Part::Field(Strand::from(
							col.name.as_str(),
						))]))],
						index: Index::Uniq {
							nulls_not_distinct: false,
						},
						kind: if create.if_not_exists {
							DefineKind::IfNotExists
						} else {
							DefineKind::Default
						},
						..Default::default()
					};
					index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
				}
				ColumnConstraint::Unique => {
					index_counter += 1;
					let idx = DefineIndexStatement {
						name: Expr::Idiom(Idiom::field(format!(
							"{safe_idx_prefix}_{}_uniq",
							col.name
						))),
						what: Expr::Table(TableName::from(tbl_name)),
						cols: vec![Expr::Idiom(Idiom(vec![Part::Field(Strand::from(
							col.name.as_str(),
						))]))],
						index: Index::Uniq {
							nulls_not_distinct: false,
						},
						kind: if create.if_not_exists {
							DefineKind::IfNotExists
						} else {
							DefineKind::Default
						},
						..Default::default()
					};
					index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
				}
				ColumnConstraint::Null => {}
			}
		}

		if let Some((target, on_delete)) = foreign_key {
			field_def.reference = Some(reference(*on_delete));
			field_def.assert = Some(existence_assertion(tbl_name, &col.name, target, !is_not_null));
		}

		if serial && matches!(field_def.default, DefineDefault::None) {
			let (sequence, default) = column_sequence(tbl_name, &col.name, create.if_not_exists);
			if create.if_not_exists {
				before.push(restart_sequence_of_new_table(tbl_name, &col.name));
			}
			exprs.push(sequence);
			field_def.default = default;
		}

		exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
	}

	for con in &create.constraints {
		match con {
			PgTableConstraint::PrimaryKey(cols) => {
				index_counter += 1;
				let idx_name = format!("{safe_idx_prefix}_{}_pk", cols.join("_"));
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(idx_name)),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: cols
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
					kind: if create.if_not_exists {
						DefineKind::IfNotExists
					} else {
						DefineKind::Default
					},
					..Default::default()
				};
				index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			PgTableConstraint::Unique(cols) => {
				index_counter += 1;
				let idx_name = format!("{safe_idx_prefix}_{}_uniq", cols.join("_"));
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(idx_name)),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: cols
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
					kind: if create.if_not_exists {
						DefineKind::IfNotExists
					} else {
						DefineKind::Default
					},
					..Default::default()
				};
				index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			PgTableConstraint::ForeignKey {
				..
			} => {}
		}
	}

	exprs.extend(index_exprs);
	let _ = index_counter;
	before.extend(exprs);
	Ok(before)
}

/// Lower a PostgreSQL CREATE INDEX statement into SurrealDB expressions.
///
/// An `hnsw` index needs the dimensions of the column it covers, which this lowering does not
/// know: see [`lower_pgsql_create_index_over`].
pub fn lower_pgsql_create_index(idx: &CreateIndexStmt) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_create_index_over(idx, &|_, _| None)
}

/// A source of the dimensions of the vector column `column` of the table `table`, if it is one.
pub type VectorDimensions<'a> = &'a dyn Fn(&str, &str) -> Option<usize>;

/// Lower a PostgreSQL CREATE INDEX statement into SurrealDB expressions, where `dimensions` says
/// how many dimensions the vector column the index covers has.
pub fn lower_pgsql_create_index_over(
	idx: &CreateIndexStmt,
	dimensions: VectorDimensions<'_>,
) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_str) = crate::namespace::resolve_pgsql_table_name(&idx.table.parts, None)?;
	let tbl = tbl_str.as_str();

	let mut cols = Vec::new();
	for col in &idx.columns {
		if let surrealdb_pgsql::ast::expr::Expr::ColumnRef(cr) = &col.expr
			&& let Some(name) = cr.fields.last()
		{
			cols.push(Expr::Idiom(Idiom(vec![Part::Field(Strand::from(name.as_str()))])));
		}
	}

	let idx_name = match &idx.name {
		Some(n) => n.clone(),
		None => {
			let col_names: Vec<String> = idx
				.columns
				.iter()
				.filter_map(|c| {
					if let surrealdb_pgsql::ast::expr::Expr::ColumnRef(cr) = &c.expr {
						cr.fields.last().cloned()
					} else {
						None
					}
				})
				.collect();
			let safe_tbl = tbl.replace('.', "_");
			format!("{safe_tbl}_{}_idx", col_names.join("_"))
		}
	};

	let method = idx.method.as_deref().map(str::to_ascii_lowercase);
	let index = match method.as_deref() {
		Some("hnsw") => {
			let column = match idx.columns.first().map(|column| &column.expr) {
				Some(surrealdb_pgsql::ast::expr::Expr::ColumnRef(column)) => {
					column.fields.last().map(String::as_str)
				}
				_ => None,
			};
			super::vector_ddl::hnsw_index(idx, column.and_then(|column| dimensions(tbl, column)))?
		}
		Some("ivfflat") => {
			return Err(PlanError::UnsupportedStatement("access method \"ivfflat\"".into()));
		}
		Some(_) | None => {
			super::vector_ddl::refuse_vector_options(idx)?;
			if idx.unique {
				Index::Uniq {
					nulls_not_distinct: false,
				}
			} else {
				Index::Idx
			}
		}
	};

	let index_stmt = DefineIndexStatement {
		name: Expr::Idiom(Idiom::field(idx_name)),
		what: Expr::Table(TableName::from(tbl)),
		cols,
		index,
		kind: if idx.if_not_exists {
			DefineKind::IfNotExists
		} else {
			DefineKind::Default
		},
		concurrently: idx.concurrently,
		..Default::default()
	};

	Ok(vec![Expr::Define(Box::new(DefineStatement::Index(index_stmt)))])
}

/// Lower a PostgreSQL CREATE VIEW statement into SurrealDB expressions.
pub fn lower_pgsql_create_view(view: &CreateViewStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, view_name_str) = crate::namespace::resolve_pgsql_table_name(&view.name.parts, None)?;
	let view_name = view_name_str.as_str();

	let mut from_tables = Vec::new();
	for tr in &view.query.from {
		if let PgTableRef::Table {
			name,
			..
		} = tr && let Ok((_, t)) = crate::namespace::resolve_pgsql_table_name(&name.parts, None)
		{
			from_tables.push(TableName::from(t));
		}
	}

	let fields = if view.query.targets.is_empty()
		|| (view.query.targets.len() == 1
			&& matches!(&view.query.targets[0].expr, surrealdb_pgsql::ast::expr::Expr::ColumnRef(cr) if cr.fields.len() == 1 && cr.fields[0] == "*"))
	{
		Fields::all()
	} else {
		let mut selectors = Vec::new();
		for target in &view.query.targets {
			let expr = lower_pgsql_expr(&target.expr)?;
			let alias = target.alias.as_ref().map(|a| Idiom::field(a.as_str()));
			selectors.push(Field::Single(Selector {
				expr,
				alias,
			}));
		}
		Fields::Select(selectors)
	};

	let cond = if let Some(wh) = &view.query.where_clause {
		Some(Cond(lower_pgsql_expr(wh)?))
	} else {
		None
	};

	let table_def = DefineTableStatement {
		name: Expr::Table(TableName::from(view_name)),
		kind: if view.or_replace {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		},
		view: Some(View {
			expr: fields,
			what: from_tables,
			cond,
			group: None,
		}),
		..Default::default()
	};

	Ok(vec![Expr::Define(Box::new(DefineStatement::Table(table_def)))])
}

/// Lower a PostgreSQL ALTER TABLE statement into SurrealDB expressions, with no description of the
/// schema: see [`lower_pgsql_alter_table_with`].
pub fn lower_pgsql_alter_table(alter: &AlterTableStmt) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_alter_table_with(alter, &EmptyCatalog)
}

/// The table of `catalog` that an `ALTER TABLE` of the table `name` in `schema` names, when the
/// catalog lists it.
fn listed_table(
	catalog: &dyn BinderCatalog,
	schema: Option<&str>,
	name: &str,
) -> Option<TableSchema> {
	if schema.is_some_and(|schema| schema != "public") {
		return None;
	}
	catalog.constraint_tables().into_iter().find(|table| table.name == name)
}

/// Lower `DROP CONSTRAINT name` of table `table`.
///
/// The name is resolved against the constraints the catalog lists for the table
/// ([`constraint_target`]):
///
/// - a `UNIQUE` constraint removes its index;
/// - a foreign key removes the `REFERENCE` and the existence `ASSERT` of its field and keeps its
///   type, so the column still reads and writes as a key;
/// - the primary key removes the `<table>_<columns>_pk` index when there is one, and is refused
///   (`0A000`) when it is the record id, which cannot be dropped;
/// - a name the table does not have is `42704`, or nothing to do under `IF EXISTS`.
///
/// A table the catalog does not list (the session may not view definitions, or the table does not
/// exist) cannot be resolved against, and the name is taken as the name of an index.
fn lower_drop_constraint(
	catalog: &dyn BinderCatalog,
	schema: Option<&str>,
	table: &str,
	name: &str,
	if_exists: bool,
) -> Result<Vec<Expr>, PlanError> {
	let remove_index = |index: &str| {
		Expr::Remove(Box::new(RemoveStatement::Index(RemoveIndexStatement {
			name: Expr::Idiom(Idiom::field(index)),
			what: Expr::Table(TableName::from(table)),
			if_exists,
		})))
	};
	let Some(listed) = listed_table(catalog, schema, table) else {
		return Ok(vec![remove_index(name)]);
	};
	match constraint_target(&listed, name) {
		Some(ConstraintTarget::Index {
			name: index,
			..
		}) => Ok(vec![remove_index(&index)]),
		Some(ConstraintTarget::ForeignKey {
			column,
		}) => Ok(vec![drop_foreign_key(table, &column)]),
		Some(ConstraintTarget::RecordId) => Err(PlanError::UnsupportedStatement(format!(
			"DROP CONSTRAINT \"{name}\": the primary key of \"{table}\" is its record id, which cannot be dropped"
		))),
		None if if_exists => Ok(Vec::new()),
		None => Err(PlanError::UndefinedConstraint {
			constraint: name.to_owned(),
			table: table.to_owned(),
		}),
	}
}

/// Lower a PostgreSQL ALTER TABLE statement into SurrealDB expressions.
///
/// `catalog` describes the schema the statement runs against. `DROP CONSTRAINT` resolves its name
/// against the constraints the catalog lists, and `ADD FOREIGN KEY` needs to know that its column
/// is declared; see [`lower_drop_constraint`] and [`lower_add_foreign_key`].
pub fn lower_pgsql_alter_table_with(
	alter: &AlterTableStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	let (schema, tbl_name_str) =
		crate::namespace::resolve_pgsql_table_name(&alter.name.parts, None)?;
	let tbl_name = tbl_name_str.as_str();

	let mut exprs = Vec::new();
	// An action reads the declarations the actions before it make, as the statement runs them in
	// order.
	let mut staged = StagedCatalog::new(catalog);

	for action in &alter.actions {
		let staged_from = exprs.len();
		match action {
			AlterTableAction::AddColumn {
				if_not_exists,
				column,
			} => {
				super::vector_ddl::check_column(&column.data_type)?;
				let domain = domain_of(&column.data_type, catalog)?;
				let own_not_null = column
					.constraints
					.iter()
					.any(|c| matches!(c, ColumnConstraint::NotNull | ColumnConstraint::PrimaryKey));
				let not_null = own_not_null;
				let foreign_key = column_foreign_key(column)?;
				if let Some((target, _)) = &foreign_key {
					check_target_exists(&staged, target, tbl_name)?;
					if domain.is_some() {
						return Err(PlanError::unsupported(format!(
							"a foreign key on the column \"{}\" of \"{tbl_name}\", which is of a domain",
							column.name
						)));
					}
				}
				let mut field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(column.name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					field_kind: Some(match &foreign_key {
						Some((target, _)) => link_kind(target, !not_null),
						None => column_kind(&column.data_type, not_null, catalog),
					}),
					kind: if *if_not_exists {
						DefineKind::IfNotExists
					} else {
						DefineKind::Default
					},
					sql_type: Some(pgsql_column_declared_type(&column.data_type, catalog)?),
					..Default::default()
				};
				for con in &column.constraints {
					match con {
						ColumnConstraint::NotNull => {
							if foreign_key.is_none() {
								field_def.assert = Some(not_null_assertion());
							}
						}
						ColumnConstraint::Default(def) => {
							let lowered = lower_pgsql_expr(def)?;
							field_def.default = DefineDefault::Set(match &foreign_key {
								Some((target, _)) => link_assignment(target, lowered, !not_null),
								None => lowered,
							});
						}
						ColumnConstraint::PrimaryKey
						| ColumnConstraint::Null
						| ColumnConstraint::Unique
						| ColumnConstraint::References {
							..
						} => {}
					}
				}
				if let Some((target, on_delete)) = &foreign_key {
					field_def.reference = Some(reference(*on_delete));
					field_def.assert =
						Some(existence_assertion(tbl_name, &column.name, target, !not_null));
				}
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			AlterTableAction::DropColumn {
				if_exists,
				name,
				..
			} => {
				let rem = RemoveFieldStatement {
					name: Expr::Idiom(Idiom::field(name.as_str())),
					table_name: Expr::Table(TableName::from(tbl_name)),
					if_exists: *if_exists,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Field(rem))));
			}
			AlterTableAction::AlterColumnType {
				name,
				data_type,
			} => {
				super::vector_ddl::check_column(data_type)?;
				domain_of(data_type, catalog)?;
				// ALTER FIELD changes the type alone, so the column keeps its DEFAULT, its
				// assertion and its permissions. A column that took NULL keeps taking it.
				let not_null = staged
					.column_shape(schema.as_deref(), tbl_name, name)
					.and_then(|shape| shape.kind)
					.is_some_and(|kind| !column_admits_none(&kind, catalog));
				let mut field = alter_column(tbl_name, name);
				field.kind = AlterKind::Set(column_kind(data_type, not_null, catalog));
				field.sql_type = AlterKind::Set(pgsql_column_declared_type(data_type, catalog)?);
				exprs.push(alter_field(field));
			}
			AlterTableAction::SetDefault {
				column,
				default,
			} => {
				let mut field = alter_column(tbl_name, column);
				let lowered = lower_pgsql_expr(default)?;
				field.default = AlterDefault::Set(
					match staged.link_table(schema.as_deref(), tbl_name, column) {
						// A default for a link column names the row it links to.
						Some(target) => link_assignment(
							&target,
							lowered,
							staged.link_admits_none(schema.as_deref(), tbl_name, column),
						),
						None => lowered,
					},
				);
				exprs.push(alter_field(field));
			}
			AlterTableAction::DropDefault {
				column,
			} => {
				let mut field = alter_column(tbl_name, column);
				field.default = AlterDefault::Drop;
				exprs.push(alter_field(field));
			}
			AlterTableAction::SetNotNull {
				column,
			} => {
				let mut field = alter_column(tbl_name, column);
				match staged.link_table(schema.as_deref(), tbl_name, column) {
					Some(target) => {
						field.kind = AlterKind::Set(link_kind(&target, false));
						field.assert =
							AlterKind::Set(existence_assertion(tbl_name, column, &target, false));
					}
					None => field.assert = AlterKind::Set(not_null_assertion()),
				}
				exprs.push(alter_field(field));
			}
			AlterTableAction::DropNotNull {
				column,
			} => {
				let mut field = alter_column(tbl_name, column);
				match staged.link_table(schema.as_deref(), tbl_name, column) {
					Some(target) => {
						field.kind = AlterKind::Set(link_kind(&target, true));
						field.assert =
							AlterKind::Set(existence_assertion(tbl_name, column, &target, true));
					}
					None => {
						field.assert = AlterKind::Drop;
						// A scalar column made `NOT NULL` by its type takes `NULL` and `NONE`
						// again. A domain column keeps its kind: the domain says whether it takes
						// NULL, as a column's own constraint cannot override.
						if let Some(shape) =
							staged.column_shape(schema.as_deref(), tbl_name, column)
							&& let Some(kind) = shape.kind
							&& !names_domain(&kind, catalog)
							&& !kind_admits_none(&kind)
						{
							field.kind =
								AlterKind::Set(Kind::either(vec![Kind::None, Kind::Null, kind]));
						}
					}
				}
				exprs.push(alter_field(field));
			}
			AlterTableAction::AddConstraint(PgTableConstraint::PrimaryKey(cols)) => {
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(format!("{tbl_name}_{}_pk", cols.join("_")))),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: cols
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			AlterTableAction::AddConstraint(PgTableConstraint::Unique(cols)) => {
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(format!("{tbl_name}_{}_uniq", cols.join("_")))),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: cols
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			AlterTableAction::DropConstraint {
				if_exists,
				name,
				..
			} => {
				exprs.extend(lower_drop_constraint(
					&staged,
					schema.as_deref(),
					tbl_name,
					name,
					*if_exists,
				)?);
			}
			AlterTableAction::AddConstraint(
				key @ PgTableConstraint::ForeignKey {
					..
				},
			) => {
				exprs.extend(lower_add_foreign_key(&staged, schema.as_deref(), tbl_name, key)?);
			}
			AlterTableAction::RenameColumn {
				..
			}
			| AlterTableAction::RenameTable {
				..
			} => {}
		}
		staged.stage(&exprs[staged_from..]);
	}

	Ok(exprs)
}

/// Lower a PostgreSQL DROP statement into SurrealDB expressions.
///
/// `DROP SCHEMA` needs the tables of the database and is refused here; see
/// [`lower_pgsql_drop_with`].
pub fn lower_pgsql_drop(drop: &PgDropStmt) -> Result<Vec<Expr>, PlanError> {
	let mut exprs = Vec::new();

	for name in &drop.names {
		let (_, obj_name_str) = crate::namespace::resolve_pgsql_table_name(&name.parts, None)?;
		let obj_name = obj_name_str.as_str();

		match drop.object_type {
			DropObjectType::Table | DropObjectType::View => {
				let rem = RemoveTableStatement {
					name: Expr::Table(TableName::from(obj_name)),
					if_exists: drop.if_exists,
					expunge: false,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Table(rem))));
			}
			DropObjectType::Index => {
				let rem = RemoveIndexStatement {
					name: Expr::Idiom(Idiom::field(obj_name)),
					what: Expr::Literal(Literal::None),
					if_exists: drop.if_exists,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Index(rem))));
			}
			DropObjectType::Type => {
				exprs.push(user_types::drop_type(
					obj_name,
					SqlTypeObject::Type,
					drop.if_exists,
					drop.cascade,
				)?);
			}
			DropObjectType::Domain => {
				exprs.push(user_types::drop_type(
					obj_name,
					SqlTypeObject::Domain,
					drop.if_exists,
					drop.cascade,
				)?);
			}
			DropObjectType::Schema => {
				return Err(PlanError::unsupported(format!(
					"DROP {:?} is not supported for typed lowering",
					drop.object_type
				)));
			}
		}
	}

	Ok(exprs)
}

/// The names of `tables` in the order they can be removed: a view before the tables it reads, and
/// otherwise by name.
///
/// A topological sort that always takes the first name among the tables no remaining table reads,
/// in time proportional to the tables and the sources the views read, times a logarithm. A source
/// that is not among `tables` is ignored.
fn removal_order(mut tables: Vec<TableSchema>) -> Vec<String> {
	tables.sort_by(|a, b| a.name.cmp(&b.name));
	tables.dedup_by(|a, b| a.name == b.name);
	let position: HashMap<&str, usize> =
		tables.iter().enumerate().map(|(at, table)| (table.name.as_str(), at)).collect();
	// `reads[at]` is what table `at` reads, and `readers[at]` how many tables left still read it.
	let reads: Vec<Vec<usize>> = tables
		.iter()
		.map(|table| {
			let mut sources: Vec<usize> = table
				.view_over
				.iter()
				.filter_map(|name| position.get(name.as_str()).copied())
				.collect();
			sources.sort_unstable();
			sources.dedup();
			sources
		})
		.collect();
	let mut readers = vec![0usize; tables.len()];
	for source in reads.iter().flatten() {
		readers[*source] += 1;
	}
	let mut ready: BTreeSet<usize> = (0..tables.len()).filter(|at| readers[*at] == 0).collect();
	let mut removed = vec![false; tables.len()];
	let mut order: Vec<String> = Vec::with_capacity(tables.len());
	while let Some(at) = ready.pop_first() {
		removed[at] = true;
		order.push(tables[at].name.clone());
		for source in &reads[at] {
			readers[*source] -= 1;
			if readers[*source] == 0 {
				ready.insert(*source);
			}
		}
	}
	// A view cycle, which the engine does not allow, leaves tables that something still reads: they
	// go last, in name order.
	order.extend(
		tables.iter().zip(&removed).filter(|(_, done)| !**done).map(|(t, _)| t.name.clone()),
	);
	order
}

/// The schemas whose contents the front end synthesises, which cannot be dropped.
const SYSTEM_SCHEMAS: [&str; 3] = ["pg_catalog", "information_schema", "pg_toast"];

/// Lower a PostgreSQL DROP statement against `catalog`, which lists the tables of the database.
///
/// A SurrealDB database is one PostgreSQL schema, `public`: `pg_namespace` and
/// `information_schema` list every user table under it, and `public.t` is the table `t`.
/// `DROP SCHEMA public CASCADE` therefore removes every table of the database, each as `REMOVE
/// TABLE` does, and then the sequences their serial and auto-increment columns draw from, so it
/// needs the role that removing the tables one by one needs, and the whole statement succeeds or
/// fails together. Without `CASCADE` it is refused (`0A000`) while the
/// database holds a table, as PostgreSQL refuses a schema that holds objects.
///
/// A name that is not `public` is no schema: it is `42P01`, or nothing to do under `IF EXISTS`.
/// The synthesised schemas (`pg_catalog`, `information_schema`, `pg_toast`) are refused (`0A000`).
/// A catalog that lists no table (a session that may not view them, or an empty database) removes
/// nothing, and a catalog that defers its checks to the
/// schema ([`BinderCatalog::defers_schema_checks`]) is lowered again once the tables are known.
///
/// Every other object type lowers as [`lower_pgsql_drop`] does.
pub fn lower_pgsql_drop_with(
	drop: &PgDropStmt,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	if !matches!(drop.object_type, DropObjectType::Schema) {
		return lower_pgsql_drop(drop);
	}
	let mut exprs = Vec::new();
	for name in &drop.names {
		let [schema] = name.parts.as_slice() else {
			return Err(PlanError::unsupported(format!(
				"DROP SCHEMA of the qualified name {}",
				name.parts.join(".")
			)));
		};
		if SYSTEM_SCHEMAS.contains(&schema.as_str()) {
			return Err(PlanError::unsupported(format!(
				"DROP SCHEMA {schema}: the schema is required by the database system"
			)));
		}
		if schema != "public" {
			if drop.if_exists {
				continue;
			}
			return Err(PlanError::UndefinedTable(schema.clone()));
		}
		let listed = catalog.constraint_tables();
		let sequences: BTreeSet<String> = listed
			.iter()
			.flat_map(|table| &table.columns)
			.filter_map(|column| column.sequence.clone())
			.collect();
		let tables = removal_order(listed);
		// Asked on every path, so a catalog that records what it is asked lowers the statement
		// again once it knows the tables.
		let deferred = catalog.defers_schema_checks();
		if !drop.cascade && !tables.is_empty() && !deferred {
			return Err(PlanError::unsupported(
				"DROP SCHEMA public: the database holds tables that depend on it; use CASCADE",
			));
		}
		if drop.cascade {
			exprs.extend(tables.iter().map(|table| {
				Expr::Remove(Box::new(RemoveStatement::Table(RemoveTableStatement {
					name: Expr::Table(TableName::from(table.as_str())),
					if_exists: true,
					expunge: false,
				})))
			}));
			// A sequence outlives the table that draws from it, so the schema's removal takes the
			// sequences of the serial and auto-increment columns along.
			exprs.extend(sequences.iter().map(|sequence| {
				Expr::Remove(Box::new(RemoveStatement::Sequence(RemoveSequenceStatement {
					name: Expr::Idiom(Idiom::field(sequence.as_str())),
					if_exists: true,
				})))
			}));
		}
	}
	Ok(exprs)
}

/// Lower a PostgreSQL DDL statement against a catalog that describes no table.
///
/// Returns the definitions it runs, one expression each, in the order they run.
#[cfg(test)]
pub(crate) fn lower_pgsql_ddl_definitions(stmt: &PgStatement) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_definitions_with(stmt, &EmptyCatalog)
}

/// Lower a PostgreSQL DDL statement against `catalog`, which `ALTER TABLE` reads for the
/// constraints of the table (see [`lower_pgsql_alter_table_with`]) and which says how many
/// dimensions a vector column has.
///
/// Returns the definitions it runs, one expression each, in the order they run.
pub(crate) fn lower_pgsql_ddl_definitions_with(
	stmt: &PgStatement,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_definitions_over(stmt, catalog, &|table, column| {
		catalog.vector_dimension(None, table, column)
	})
}

/// Lower a PostgreSQL DDL statement as [`lower_pgsql_ddl_definitions_with`] does, where
/// `dimensions` says how many dimensions a vector column has.
fn lower_pgsql_ddl_definitions_over(
	stmt: &PgStatement,
	catalog: &dyn BinderCatalog,
	dimensions: VectorDimensions<'_>,
) -> Result<Vec<Expr>, PlanError> {
	match stmt {
		PgStatement::CreateTable(s) => {
			check_create_targets(s, catalog)?;
			lower_pgsql_create_table_with(s, catalog)
		}
		PgStatement::CreateType(s) => user_types::lower_create_enum(s),
		PgStatement::AlterType(s) => user_types::lower_alter_enum(s),
		PgStatement::CreateIndex(s) => lower_pgsql_create_index_over(s, dimensions),
		PgStatement::CreateExtension(s) => super::vector_ddl::lower_create_extension(s),
		PgStatement::CreateView(s) => lower_pgsql_create_view(s),
		PgStatement::AlterTable(s) => lower_pgsql_alter_table_with(s, catalog),
		PgStatement::Drop(s) => lower_pgsql_drop_with(s, catalog),
		PgStatement::CreateDomain(s) => user_types::lower_create_domain(s, catalog),
		PgStatement::AlterDomain(s) => user_types::lower_alter_domain(s, catalog),
		PgStatement::CreateCompositeType(s) => user_types::lower_create_composite(s, catalog),
		PgStatement::Copy(_)
		| PgStatement::Select(_)
		| PgStatement::Insert(_)
		| PgStatement::Update(_)
		| PgStatement::Delete(_)
		| PgStatement::Truncate(_)
		| PgStatement::CreateSchema(_)
		| PgStatement::Transaction(_)
		| PgStatement::Set(_)
		| PgStatement::Show(_)
		| PgStatement::Reset(_)
		| PgStatement::Explain(_)
		| PgStatement::Prepare(_)
		| PgStatement::Execute(_)
		| PgStatement::Deallocate(_)
		| PgStatement::CreatePropertyGraph(_)
		| PgStatement::DropPropertyGraph(_)
		| PgStatement::Empty => Err(PlanError::unsupported(format!(
			"PostgreSQL statement not recognized as DDL: {stmt:?}"
		))),
	}
}

/// Lower a MySQL CREATE TABLE statement into SurrealDB expressions.
pub fn lower_mysql_create_table(create: &MyCreateTableStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_name_str) = crate::namespace::resolve_mysql_table_name(&create.name.parts, None)?;
	let tbl_name = tbl_name_str.as_str();

	let mut exprs = Vec::new();
	let mut index_exprs = Vec::new();
	// What must run before the table is defined.
	let mut before = Vec::new();

	let table_def = DefineTableStatement {
		name: Expr::Table(TableName::from(tbl_name)),
		kind: if create.if_not_exists {
			DefineKind::IfNotExists
		} else {
			DefineKind::Default
		},
		full: true,
		table_type: TableType::Normal,
		..Default::default()
	};
	exprs.push(Expr::Define(Box::new(DefineStatement::Table(table_def))));

	for col in &create.columns {
		let is_id = col.name == "id";
		let is_not_null = is_id || col.not_null || col.primary_key || col.auto_increment;
		check_mysql_set_limit(&col.data_type)?;
		let base_kind = key_column_kind(&col.name, map_mysql_type(&col.data_type));
		let field_kind = if is_not_null || matches!(base_kind, Kind::Any) {
			base_kind
		} else {
			Kind::either(vec![Kind::None, Kind::Null, base_kind])
		};

		let mut field_def = DefineFieldStatement {
			name: Expr::Idiom(Idiom::field(col.name.as_str())),
			what: Expr::Table(TableName::from(tbl_name)),
			flexible: holds_object(&field_kind),
			field_kind: Some(field_kind),
			kind: if create.if_not_exists {
				DefineKind::IfNotExists
			} else {
				DefineKind::Default
			},
			sql_type: Some(mysql_declared_type(&col.data_type)?),
			..Default::default()
		};

		if col.not_null {
			field_def.assert = Some(not_null_assertion());
		}

		if let Some(def) = &col.default {
			field_def.default = DefineDefault::Set(lower_mysql_expr(def)?);
		} else if col.auto_increment {
			let (sequence, default) = column_sequence(tbl_name, &col.name, create.if_not_exists);
			if create.if_not_exists {
				before.push(restart_sequence_of_new_table(tbl_name, &col.name));
			}
			exprs.push(sequence);
			field_def.default = default;
		}

		if col.primary_key {
			let idx = DefineIndexStatement {
				name: Expr::Idiom(Idiom::field(format!("{tbl_name}_{}_pk", col.name))),
				what: Expr::Table(TableName::from(tbl_name)),
				cols: vec![Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.name.as_str()))]))],
				index: Index::Uniq {
					nulls_not_distinct: false,
				},
				kind: if create.if_not_exists {
					DefineKind::IfNotExists
				} else {
					DefineKind::Default
				},
				..Default::default()
			};
			index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
		}

		if col.unique {
			let idx = DefineIndexStatement {
				name: Expr::Idiom(Idiom::field(format!("{tbl_name}_{}_uniq", col.name))),
				what: Expr::Table(TableName::from(tbl_name)),
				cols: vec![Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.name.as_str()))]))],
				index: Index::Uniq {
					nulls_not_distinct: false,
				},
				kind: if create.if_not_exists {
					DefineKind::IfNotExists
				} else {
					DefineKind::Default
				},
				..Default::default()
			};
			index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
		}

		exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
	}

	for con in &create.constraints {
		match con {
			MyTableConstraint::PrimaryKey {
				columns,
				..
			} => {
				let idx_name = format!("{tbl_name}_{}_pk", columns.join("_"));
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(idx_name)),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: columns
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
					kind: if create.if_not_exists {
						DefineKind::IfNotExists
					} else {
						DefineKind::Default
					},
					..Default::default()
				};
				index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			MyTableConstraint::Unique {
				name,
				columns,
				..
			} => {
				let idx_name = match name {
					Some(n) => n.clone(),
					None => format!("{tbl_name}_{}_uniq", columns.join("_")),
				};
				let idx = DefineIndexStatement {
					name: Expr::Idiom(Idiom::field(idx_name)),
					what: Expr::Table(TableName::from(tbl_name)),
					cols: columns
						.iter()
						.map(|c| Expr::Idiom(Idiom(vec![Part::Field(Strand::from(c.as_str()))])))
						.collect(),
					index: Index::Uniq {
						nulls_not_distinct: false,
					},
					kind: if create.if_not_exists {
						DefineKind::IfNotExists
					} else {
						DefineKind::Default
					},
					..Default::default()
				};
				index_exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
			}
			MyTableConstraint::ForeignKey {
				columns,
				..
			} => {
				for col in columns {
					// ALTER FIELD adds the reference to the column the table defines, which keeps
					// its type, default and assertion.
					let mut field = alter_column(tbl_name, col);
					field.reference = AlterKind::Set(Reference {
						on_delete: ReferenceDeleteStrategy::Reject,
						materialize: None,
					});
					exprs.push(alter_field(field));
				}
			}
		}
	}

	exprs.extend(index_exprs);

	before.extend(exprs);
	Ok(before)
}

/// Lower a MySQL DROP TABLE statement into SurrealDB expressions.
pub fn lower_mysql_drop_table(drop: &MyDropTableStmt) -> Result<Vec<Expr>, PlanError> {
	let mut exprs = Vec::new();

	for name in &drop.names {
		let (_, tbl_name_str) = crate::namespace::resolve_mysql_table_name(&name.parts, None)?;
		let tbl_name = tbl_name_str.as_str();

		let rem = RemoveTableStatement {
			name: Expr::Table(TableName::from(tbl_name)),
			if_exists: drop.if_exists,
			expunge: false,
		};
		exprs.push(Expr::Remove(Box::new(RemoveStatement::Table(rem))));
	}

	Ok(exprs)
}

/// Lower a MySQL `USE` statement into a SurrealDB `UseStatement`.
pub fn lower_mysql_use(
	use_stmt: &surrealdb_mysql::ast::stmt::UseStmt,
) -> Result<surrealdb_expr::expr::statements::UseStatement, PlanError> {
	if use_stmt.database.trim().is_empty() {
		return Err(PlanError::Custom("empty database name in USE".into()));
	}
	Ok(surrealdb_expr::expr::statements::UseStatement::Db(Expr::Idiom(Idiom::field(
		use_stmt.database.clone(),
	))))
}

/// Lower a MySQL `CREATE DATABASE` statement into SurrealDB expressions.
pub fn lower_mysql_create_database(create: &MyCreateDatabaseStmt) -> Result<Vec<Expr>, PlanError> {
	let def = DefineDatabaseStatement {
		name: Expr::Idiom(Idiom::field(create.name.as_str())),
		kind: if create.if_not_exists {
			DefineKind::IfNotExists
		} else {
			DefineKind::Default
		},
		..Default::default()
	};
	Ok(vec![Expr::Define(Box::new(DefineStatement::Database(def)))])
}

/// Lower a MySQL `DROP DATABASE` statement into SurrealDB expressions.
pub fn lower_mysql_drop_database(drop: &MyDropDatabaseStmt) -> Result<Vec<Expr>, PlanError> {
	let rem = RemoveDatabaseStatement {
		name: Expr::Idiom(Idiom::field(drop.name.as_str())),
		if_exists: drop.if_exists,
		..Default::default()
	};
	Ok(vec![Expr::Remove(Box::new(RemoveStatement::Database(rem)))])
}

/// Lower a MySQL `CREATE INDEX` statement into SurrealDB expressions.
pub fn lower_mysql_create_index(idx: &MyCreateIndexStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_str) = crate::namespace::resolve_mysql_table_name(&idx.table.parts, None)?;
	let tbl = tbl_str.as_str();

	let mut cols = Vec::new();
	for col in &idx.columns {
		cols.push(Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.as_str()))])));
	}

	let def = DefineIndexStatement {
		name: Expr::Idiom(Idiom::field(idx.name.as_str())),
		what: Expr::Table(TableName::from(tbl)),
		cols,
		index: if idx.unique {
			Index::Uniq {
				nulls_not_distinct: false,
			}
		} else {
			Index::Idx
		},
		kind: DefineKind::Default,
		..Default::default()
	};
	Ok(vec![Expr::Define(Box::new(DefineStatement::Index(def)))])
}

/// Lower a MySQL `DROP INDEX` statement into SurrealDB expressions.
pub fn lower_mysql_drop_index(drop: &MyDropIndexStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_str) = crate::namespace::resolve_mysql_table_name(&drop.table.parts, None)?;
	let tbl = tbl_str.as_str();

	let rem = RemoveIndexStatement {
		name: Expr::Idiom(Idiom::field(drop.name.as_str())),
		what: Expr::Table(TableName::from(tbl)),
		if_exists: false,
	};
	Ok(vec![Expr::Remove(Box::new(RemoveStatement::Index(rem)))])
}

/// Lower a MySQL `CREATE VIEW` statement into SurrealDB expressions.
pub fn lower_mysql_create_view(view: &MyCreateViewStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, view_name_str) = crate::namespace::resolve_mysql_table_name(&view.name.parts, None)?;
	let view_name = view_name_str.as_str();

	let mut from_tables = Vec::new();
	if let Some(MyTableRef::Table {
		name,
		..
	}) = &view.query.from
		&& let Ok((_, t)) = crate::namespace::resolve_mysql_table_name(&name.parts, None)
	{
		from_tables.push(TableName::from(t));
	}

	let fields = if view.query.projection.is_empty()
		|| (view.query.projection.len() == 1
			&& matches!(
				&view.query.projection[0],
				surrealdb_mysql::ast::stmt::SelectItem::Wildcard(_)
			)) {
		Fields::all()
	} else {
		let mut selectors = Vec::new();
		for item in &view.query.projection {
			match item {
				surrealdb_mysql::ast::stmt::SelectItem::Wildcard(_) => {
					selectors.push(Field::All);
				}
				surrealdb_mysql::ast::stmt::SelectItem::Expr {
					expr,
					alias,
					..
				} => {
					let lowered = lower_mysql_expr(expr)?;
					let a = alias.as_ref().map(|s| Idiom::field(s.as_str()));
					selectors.push(Field::Single(Selector {
						expr: lowered,
						alias: a,
					}));
				}
				surrealdb_mysql::ast::stmt::SelectItem::QualifiedWildcard(parts, _) => {
					selectors.push(Field::Single(Selector {
						expr: Expr::Idiom(Idiom(
							parts
								.iter()
								.map(|part| Part::Field(Strand::from(part.as_str())))
								.chain(std::iter::once(Part::All))
								.collect(),
						)),
						alias: None,
					}));
				}
			}
		}
		Fields::Select(selectors)
	};

	let cond = if let Some(wh) = &view.query.where_clause {
		Some(Cond(lower_mysql_expr(wh)?))
	} else {
		None
	};

	let table_def = DefineTableStatement {
		name: Expr::Table(TableName::from(view_name)),
		kind: if view.or_replace {
			DefineKind::Overwrite
		} else {
			DefineKind::Default
		},
		view: Some(View {
			expr: fields,
			what: from_tables,
			cond,
			group: None,
		}),
		..Default::default()
	};

	Ok(vec![Expr::Define(Box::new(DefineStatement::Table(table_def)))])
}

/// Lower a MySQL `DROP VIEW` statement into SurrealDB expressions.
pub fn lower_mysql_drop_view(drop: &MyDropViewStmt) -> Result<Vec<Expr>, PlanError> {
	let mut exprs = Vec::new();
	for name in &drop.names {
		let (_, vname_str) = crate::namespace::resolve_mysql_table_name(&name.parts, None)?;
		let rem = RemoveTableStatement {
			name: Expr::Table(TableName::from(vname_str.as_str())),
			if_exists: drop.if_exists,
			expunge: false,
		};
		exprs.push(Expr::Remove(Box::new(RemoveStatement::Table(rem))));
	}
	Ok(exprs)
}

/// Lower a MySQL `ALTER TABLE` statement into SurrealDB expressions.
pub fn lower_mysql_alter_table(alter: &MyAlterTableStmt) -> Result<Vec<Expr>, PlanError> {
	let (_, tbl_name_str) = crate::namespace::resolve_mysql_table_name(&alter.table.parts, None)?;
	let tbl_name = tbl_name_str.as_str();

	let mut exprs = Vec::new();

	for action in &alter.actions {
		match action {
			MyAlterTableAction::AddColumn(col) => {
				check_mysql_set_limit(&col.data_type)?;
				let base_kind = key_column_kind(&col.name, map_mysql_type(&col.data_type));
				let is_not_null = col.name == "id" || col.not_null || col.primary_key;
				let field_kind = if is_not_null || matches!(base_kind, Kind::Any) {
					base_kind
				} else {
					Kind::either(vec![Kind::None, Kind::Null, base_kind])
				};

				let mut field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(col.name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					field_kind: Some(field_kind),
					kind: DefineKind::Default,
					sql_type: Some(mysql_declared_type(&col.data_type)?),
					..Default::default()
				};
				if let Some(def) = &col.default {
					field_def.default = DefineDefault::Set(lower_mysql_expr(def)?);
				}
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			MyAlterTableAction::DropColumn {
				name,
			} => {
				let rem = RemoveFieldStatement {
					name: Expr::Idiom(Idiom::field(name.as_str())),
					table_name: Expr::Table(TableName::from(tbl_name)),
					if_exists: false,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Field(rem))));
			}
			MyAlterTableAction::ModifyColumn(col) => {
				check_mysql_set_limit(&col.data_type)?;
				let base_kind = key_column_kind(&col.name, map_mysql_type(&col.data_type));
				let is_not_null = col.name == "id" || col.not_null || col.primary_key;
				let field_kind = if is_not_null || matches!(base_kind, Kind::Any) {
					base_kind
				} else {
					Kind::either(vec![Kind::None, Kind::Null, base_kind])
				};

				let mut field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(col.name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					field_kind: Some(field_kind),
					kind: DefineKind::Overwrite,
					sql_type: Some(mysql_declared_type(&col.data_type)?),
					..Default::default()
				};
				if let Some(def) = &col.default {
					field_def.default = DefineDefault::Set(lower_mysql_expr(def)?);
				}
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			MyAlterTableAction::ChangeColumn {
				old_name,
				new_def,
			} => {
				let rem = RemoveFieldStatement {
					name: Expr::Idiom(Idiom::field(old_name.as_str())),
					table_name: Expr::Table(TableName::from(tbl_name)),
					if_exists: true,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Field(rem))));

				check_mysql_set_limit(&new_def.data_type)?;
				let base_kind = key_column_kind(&new_def.name, map_mysql_type(&new_def.data_type));
				let is_not_null = new_def.name == "id" || new_def.not_null || new_def.primary_key;
				let field_kind = if is_not_null || matches!(base_kind, Kind::Any) {
					base_kind
				} else {
					Kind::either(vec![Kind::None, Kind::Null, base_kind])
				};

				let mut field_def = DefineFieldStatement {
					name: Expr::Idiom(Idiom::field(new_def.name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					field_kind: Some(field_kind),
					kind: DefineKind::Default,
					sql_type: Some(mysql_declared_type(&new_def.data_type)?),
					..Default::default()
				};
				if let Some(def) = &new_def.default {
					field_def.default = DefineDefault::Set(lower_mysql_expr(def)?);
				}
				exprs.push(Expr::Define(Box::new(DefineStatement::Field(Box::new(field_def)))));
			}
			MyAlterTableAction::AddConstraint(c) => match c {
				MyTableConstraint::PrimaryKey {
					columns,
					..
				} => {
					let idx = DefineIndexStatement {
						name: Expr::Idiom(Idiom::field(format!("{tbl_name}_pk"))),
						what: Expr::Table(TableName::from(tbl_name)),
						cols: columns
							.iter()
							.map(|col| {
								Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.as_str()))]))
							})
							.collect(),
						index: Index::Uniq {
							nulls_not_distinct: false,
						},
						kind: DefineKind::Default,
						..Default::default()
					};
					exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
				}
				MyTableConstraint::Unique {
					name,
					columns,
					..
				} => {
					let idx_name = name.clone().unwrap_or_else(|| format!("{tbl_name}_uniq"));
					let idx = DefineIndexStatement {
						name: Expr::Idiom(Idiom::field(idx_name)),
						what: Expr::Table(TableName::from(tbl_name)),
						cols: columns
							.iter()
							.map(|col| {
								Expr::Idiom(Idiom(vec![Part::Field(Strand::from(col.as_str()))]))
							})
							.collect(),
						index: Index::Uniq {
							nulls_not_distinct: false,
						},
						kind: DefineKind::Default,
						..Default::default()
					};
					exprs.push(Expr::Define(Box::new(DefineStatement::Index(idx))));
				}
				MyTableConstraint::ForeignKey {
					..
				} => {}
			},
			MyAlterTableAction::DropPrimaryKey => {
				let rem = RemoveIndexStatement {
					name: Expr::Idiom(Idiom::field(format!("{tbl_name}_pk"))),
					what: Expr::Table(TableName::from(tbl_name)),
					if_exists: true,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Index(rem))));
			}
			MyAlterTableAction::DropIndex {
				name,
			} => {
				let rem = RemoveIndexStatement {
					name: Expr::Idiom(Idiom::field(name.as_str())),
					what: Expr::Table(TableName::from(tbl_name)),
					if_exists: true,
				};
				exprs.push(Expr::Remove(Box::new(RemoveStatement::Index(rem))));
			}
			MyAlterTableAction::RenameTable {
				new_name,
			} => {
				let (_, new_tbl_str) =
					crate::namespace::resolve_mysql_table_name(&new_name.parts, None)?;
				let table_def = DefineTableStatement {
					name: Expr::Table(TableName::from(new_tbl_str.as_str())),
					kind: DefineKind::Default,
					full: true,
					table_type: TableType::Normal,
					..Default::default()
				};
				exprs.push(Expr::Define(Box::new(DefineStatement::Table(table_def))));
			}
		}
	}

	Ok(exprs)
}

/// Lower a MySQL DDL statement.
///
/// Returns the definitions it runs, one expression each, in the order they run.
pub(crate) fn lower_mysql_ddl_definitions(stmt: &MyStatement) -> Result<Vec<Expr>, PlanError> {
	match stmt {
		MyStatement::CreateTable(s) => lower_mysql_create_table(s),
		MyStatement::DropTable(s) => lower_mysql_drop_table(s),
		MyStatement::CreateIndex(s) => lower_mysql_create_index(s),
		MyStatement::DropIndex(s) => lower_mysql_drop_index(s),
		MyStatement::CreateView(s) => lower_mysql_create_view(s),
		MyStatement::DropView(s) => lower_mysql_drop_view(s),
		MyStatement::AlterTable(s) => lower_mysql_alter_table(s),
		MyStatement::CreateDatabase(s) => lower_mysql_create_database(s),
		MyStatement::DropDatabase(s) => lower_mysql_drop_database(s),
		MyStatement::Select(_)
		| MyStatement::Insert(_)
		| MyStatement::Update(_)
		| MyStatement::Delete(_)
		| MyStatement::Replace(_)
		| MyStatement::Truncate(_)
		| MyStatement::CreateUser(_)
		| MyStatement::DropUser(_)
		| MyStatement::Grant(_)
		| MyStatement::Revoke(_)
		| MyStatement::LockTables(_)
		| MyStatement::UnlockTables(_)
		| MyStatement::Prepare(_)
		| MyStatement::Execute(_)
		| MyStatement::Deallocate(_)
		| MyStatement::Use(_)
		| MyStatement::Set(_)
		| MyStatement::Show(_)
		| MyStatement::Describe(_)
		| MyStatement::Explain(_)
		| MyStatement::Transaction(_) => {
			Err(PlanError::unsupported(format!("MySQL statement not recognized as DDL: {stmt:?}")))
		}
	}
}

/// Lower a PostgreSQL DDL statement into the expression that answers it.
///
/// The result holds exactly one expression, so a caller that runs every returned expression as a
/// statement of its own answers the one SQL statement once. A statement that lowers to several
/// definitions (a table with its fields and indexes, an `ALTER TABLE` with several actions, a
/// `DROP` of several tables) lowers to a block of them: a definition that fails fails the
/// statement as a whole, and none of its definitions is committed.
pub fn lower_pgsql_ddl(stmt: &PgStatement) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_with(stmt, &EmptyCatalog)
}

/// Lower a PostgreSQL DDL statement against `catalog` into the expression that answers it, as
/// [`lower_pgsql_ddl`] does; `ALTER TABLE` reads `catalog` for the constraints of the table.
pub fn lower_pgsql_ddl_with(
	stmt: &PgStatement,
	catalog: &dyn BinderCatalog,
) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_definitions_with(stmt, catalog)
		.map(|definitions| vec![one_statement(definitions)])
}

/// Lower a PostgreSQL DDL statement as [`lower_pgsql_ddl`] does, where `dimensions` says how many
/// dimensions a vector column has, which an `hnsw` index needs.
pub fn lower_pgsql_ddl_over(
	stmt: &PgStatement,
	dimensions: VectorDimensions<'_>,
) -> Result<Vec<Expr>, PlanError> {
	lower_pgsql_ddl_definitions_over(stmt, &EmptyCatalog, dimensions)
		.map(|definitions| vec![one_statement(definitions)])
}

/// Lower a MySQL DDL statement into the expression that answers it.
///
/// The result holds exactly one expression, as [`lower_pgsql_ddl`] does, and a statement that
/// lowers to several definitions (a table with its fields and indexes, several tables dropped at
/// once) lowers to a block of them with the same all-or-nothing outcome.
pub fn lower_mysql_ddl(stmt: &MyStatement) -> Result<Vec<Expr>, PlanError> {
	lower_mysql_ddl_definitions(stmt).map(|definitions| vec![one_statement(definitions)])
}

/// The single expression that runs `definitions`: the definition itself when there is exactly
/// one, otherwise a block of them in order. A statement that lowers to no definition is an empty
/// block, which answers `NONE`.
fn one_statement(mut definitions: Vec<Expr>) -> Expr {
	if definitions.len() == 1
		&& let Some(definition) = definitions.pop()
	{
		return definition;
	}
	Expr::Block(Box::new(surrealdb_expr::expr::Block(definitions)))
}

#[cfg(test)]
mod statement_tests;
mod user_types;
