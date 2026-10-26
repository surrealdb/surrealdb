//! Result descriptors, static typing, and parameter description for SQL statements.

use std::collections::HashMap;

mod common_type;
pub mod pgsql;

use self::common_type::is_null_literal;
use crate::catalog::BinderCatalog;
use crate::dialect::{DialectPolicy, SqlDialect};
use crate::functions::FunctionRegistry;
use crate::ir::{
	BoundBinaryOp, BoundExpr, BoundLiteral, BoundSelect, BoundTableSource, BoundTarget,
	BoundUnaryOp, BoundWindowFrameBound,
};
use crate::output_keys::output_keys;

/// Abstract strongly-typed SQL data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SqlType {
	#[default]
	Unknown,
	Bool,
	Int2,
	Int4,
	Int8,
	Float4,
	Float8,
	Numeric,
	Text,
	Bytea,
	Date,
	Time,
	Timestamp,
	TimestampTz,
	Interval,
	Json,
	/// A one-dimensional array of the element type.
	Array(&'static SqlType),
	Uuid,
	Char,
	Name,
	Varchar,
	Bpchar,
	Timetz,
	Inet,
	Cidr,
	MacAddr,
	Oid,
	Regclass,
	Record,
	/// `int2vector`: the values of an array of `int2`, numbered from zero.
	Int2Vector,
	/// `oidvector`: the values of an array of `oid`, numbered from zero.
	OidVector,
	/// A `vector(n)` of the pgvector extension: a fixed-length array of floats with its own wire
	/// type and text form (`[1,2,3]`).
	Vector,
}

/// The wire OID of the `vector` type and of an array of it. PostgreSQL assigns an extension's type
/// an OID when it is created, and clients find it by name in `pg_type`, so any OID outside the
/// built-in range serves.
pub const VECTOR_OID: i32 = 16400;
/// The wire OID of an array of `vector`.
pub const VECTOR_ARRAY_OID: i32 = 16401;

impl SqlType {
	/// A scalar PostgreSQL OID that this descriptor can represent exactly.
	pub fn from_pg_oid(oid: i32) -> Option<Self> {
		match oid {
			16 => Some(Self::Bool),
			17 => Some(Self::Bytea),
			18 => Some(Self::Char),
			19 => Some(Self::Name),
			20 => Some(Self::Int8),
			21 => Some(Self::Int2),
			23 => Some(Self::Int4),
			25 => Some(Self::Text),
			26 => Some(Self::Oid),
			114 => Some(Self::Json),
			650 => Some(Self::Cidr),
			700 => Some(Self::Float4),
			701 => Some(Self::Float8),
			829 => Some(Self::MacAddr),
			869 => Some(Self::Inet),
			1042 => Some(Self::Bpchar),
			1043 => Some(Self::Varchar),
			1082 => Some(Self::Date),
			1083 => Some(Self::Time),
			1114 => Some(Self::Timestamp),
			1184 => Some(Self::TimestampTz),
			1186 => Some(Self::Interval),
			1266 => Some(Self::Timetz),
			1700 => Some(Self::Numeric),
			2205 => Some(Self::Regclass),
			2249 => Some(Self::Record),
			22 => Some(Self::Int2Vector),
			30 => Some(Self::OidVector),
			2950 => Some(Self::Uuid),
			VECTOR_OID => Some(Self::Vector),
			_ => None,
		}
	}

	/// PostgreSQL wire type OID.
	pub fn to_pg_oid(&self) -> i32 {
		match self {
			Self::Unknown => 0, // 0 = unspecified
			Self::Bool => 16,
			Self::Bytea => 17,
			Self::Char => 18,
			Self::Name => 19,
			Self::Int8 => 20,
			Self::Int2 => 21,
			Self::Int4 => 23,
			Self::Text => 25,
			Self::Oid => 26,
			Self::Json => 114,
			Self::Cidr => 650,
			Self::Float4 => 700,
			Self::Float8 => 701,
			Self::MacAddr => 829,
			Self::Inet => 869,
			Self::Bpchar => 1042,
			Self::Varchar => 1043,
			Self::Date => 1082,
			Self::Time => 1083,
			Self::Timestamp => 1114,
			Self::TimestampTz => 1184,
			Self::Interval => 1186,
			Self::Timetz => 1266,
			Self::Numeric => 1700,
			Self::Regclass => 2205,
			Self::Record => 2249,
			Self::Int2Vector => 22,
			Self::OidVector => 30,
			Self::Vector => VECTOR_OID,
			Self::Array(elem) => elem.pg_array_oid(),
			Self::Uuid => 2950,
		}
	}

	/// PostgreSQL wire OID of a one-dimensional array of this type, or `anyarray` when
	/// PostgreSQL has no array type for it.
	fn pg_array_oid(self) -> i32 {
		match self {
			Self::Bool => 1000,
			Self::Bytea => 1001,
			Self::Char => 1002,
			Self::Name => 1003,
			Self::Int2 => 1005,
			Self::Int4 => 1007,
			Self::Text => 1009,
			Self::Bpchar => 1014,
			Self::Varchar => 1015,
			Self::Int8 => 1016,
			Self::Float4 => 1021,
			Self::Float8 => 1022,
			Self::Oid => 1028,
			Self::MacAddr => 1040,
			Self::Inet => 1041,
			Self::Cidr => 651,
			Self::Timestamp => 1115,
			Self::Date => 1182,
			Self::Time => 1183,
			Self::TimestampTz => 1185,
			Self::Interval => 1187,
			Self::Numeric => 1231,
			Self::Timetz => 1270,
			Self::Json => 199,
			Self::Uuid => 2951,
			Self::Regclass => 2210,
			Self::Record => 2287,
			Self::Int2Vector => 1006,
			Self::OidVector => 1013,
			Self::Vector => VECTOR_ARRAY_OID,
			Self::Unknown | Self::Array(_) => 2277,
		}
	}

	/// A one-dimensional array of `elem`, or `None` when `elem` is unknown or itself an array.
	pub fn array_of(elem: Self) -> Option<Self> {
		let elem: &'static Self = match elem {
			Self::Bool => &Self::Bool,
			Self::Int2 => &Self::Int2,
			Self::Int4 => &Self::Int4,
			Self::Int8 => &Self::Int8,
			Self::Float4 => &Self::Float4,
			Self::Float8 => &Self::Float8,
			Self::Numeric => &Self::Numeric,
			Self::Text => &Self::Text,
			Self::Bytea => &Self::Bytea,
			Self::Date => &Self::Date,
			Self::Time => &Self::Time,
			Self::Timestamp => &Self::Timestamp,
			Self::TimestampTz => &Self::TimestampTz,
			Self::Interval => &Self::Interval,
			Self::Json => &Self::Json,
			Self::Uuid => &Self::Uuid,
			Self::Char => &Self::Char,
			Self::Name => &Self::Name,
			Self::Varchar => &Self::Varchar,
			Self::Bpchar => &Self::Bpchar,
			Self::Timetz => &Self::Timetz,
			Self::Inet => &Self::Inet,
			Self::Cidr => &Self::Cidr,
			Self::MacAddr => &Self::MacAddr,
			Self::Oid => &Self::Oid,
			Self::Regclass => &Self::Regclass,
			Self::Record => &Self::Record,
			Self::Vector => &Self::Vector,
			Self::Unknown | Self::Array(_) | Self::Int2Vector | Self::OidVector => return None,
		};
		Some(Self::Array(elem))
	}

	/// PostgreSQL type length in bytes (-1 for variable length).
	pub fn pg_typlen(&self) -> i16 {
		match self {
			Self::Bool | Self::Char => 1,
			Self::Int2 => 2,
			Self::Int4 | Self::Float4 | Self::Date | Self::Oid | Self::Regclass => 4,
			Self::MacAddr => 6,
			Self::Int8 | Self::Float8 | Self::Timestamp | Self::TimestampTz | Self::Time => 8,
			Self::Timetz => 12,
			Self::Interval | Self::Uuid => 16,
			Self::Name => 64,
			Self::Unknown
			| Self::Numeric
			| Self::Text
			| Self::Varchar
			| Self::Bpchar
			| Self::Bytea
			| Self::Json
			| Self::Array(_)
			| Self::Inet
			| Self::Cidr
			| Self::Int2Vector
			| Self::OidVector
			| Self::Vector
			| Self::Record => -1,
		}
	}

	/// MySQL wire field type.
	pub fn to_mysql_type(&self) -> u8 {
		match self {
			Self::Unknown => 0xfd,                           // MYSQL_TYPE_VAR_STRING
			Self::Bool => 0x01,                              // MYSQL_TYPE_TINY
			Self::Int2 => 0x02,                              // MYSQL_TYPE_SHORT
			Self::Int4 | Self::Oid | Self::Regclass => 0x03, // MYSQL_TYPE_LONG
			Self::Int8 => 0x08,                              // MYSQL_TYPE_LONGLONG
			Self::Float4 => 0x04,                            // MYSQL_TYPE_FLOAT
			Self::Float8 => 0x05,                            // MYSQL_TYPE_DOUBLE
			Self::Numeric => 0xf6,                           // MYSQL_TYPE_NEWDECIMAL
			Self::Text
			| Self::Name
			| Self::Varchar
			| Self::Bpchar
			| Self::Char
			| Self::Inet
			| Self::Cidr
			| Self::MacAddr => 0xfd, // MYSQL_TYPE_VAR_STRING
			Self::Bytea => 0xfc,                             // MYSQL_TYPE_BLOB
			Self::Date => 0x0a,                              // MYSQL_TYPE_DATE
			Self::Time | Self::Timetz => 0x0b,               // MYSQL_TYPE_TIME
			Self::Timestamp | Self::TimestampTz => 0x07,     // MYSQL_TYPE_TIMESTAMP
			Self::Interval => 0xfd,                          // MYSQL_TYPE_VAR_STRING
			Self::Uuid => 0xfd,                              // MYSQL_TYPE_VAR_STRING
			Self::Json
			| Self::Array(_)
			| Self::Vector
			| Self::Int2Vector
			| Self::OidVector
			| Self::Record => 0xf5, // MYSQL_TYPE_JSON
		}
	}

	/// The name `information_schema.columns.data_type` gives this type, which
	/// [`Self::from_name`] reads back as the same type. An array is its element's name followed
	/// by `[]`. [`Self::Unknown`] has no name.
	pub fn name(&self) -> Option<String> {
		let name = match self {
			Self::Unknown => return None,
			Self::Array(elem) => return elem.name().map(|elem| format!("{elem}[]")),
			Self::Bool => "boolean",
			Self::Int2 => "smallint",
			Self::Int4 => "integer",
			Self::Int8 => "bigint",
			Self::Float4 => "real",
			Self::Float8 => "double precision",
			Self::Numeric => "numeric",
			Self::Text => "text",
			Self::Bytea => "bytea",
			Self::Date => "date",
			Self::Time => "time without time zone",
			Self::Timestamp => "timestamp without time zone",
			Self::TimestampTz => "timestamp with time zone",
			Self::Interval => "interval",
			Self::Json => "jsonb",
			Self::Uuid => "uuid",
			Self::Char => "\"char\"",
			Self::Name => "name",
			Self::Varchar => "character varying",
			Self::Bpchar => "character",
			Self::Timetz => "time with time zone",
			Self::Inet => "inet",
			Self::Cidr => "cidr",
			Self::MacAddr => "macaddr",
			Self::Oid => "oid",
			Self::Regclass => "regclass",
			Self::Record => "record",
			Self::Int2Vector => "int2vector",
			Self::OidVector => "oidvector",
			Self::Vector => "vector",
		};
		Some(name.to_owned())
	}

	/// PostgreSQL's internal type name (`pg_type.typname`, `information_schema.columns.udt_name`):
	/// `int8`, `bool`, `timestamptz`, and `_text` for an array of `text`. [`Self::Unknown`] has
	/// no name.
	pub fn udt_name(&self) -> Option<String> {
		let name = match self {
			Self::Unknown => return None,
			Self::Array(elem) => return elem.udt_name().map(|elem| format!("_{elem}")),
			Self::Bool => "bool",
			Self::Int2 => "int2",
			Self::Int4 => "int4",
			Self::Int8 => "int8",
			Self::Float4 => "float4",
			Self::Float8 => "float8",
			Self::Numeric => "numeric",
			Self::Text => "text",
			Self::Bytea => "bytea",
			Self::Date => "date",
			Self::Time => "time",
			Self::Timestamp => "timestamp",
			Self::TimestampTz => "timestamptz",
			Self::Interval => "interval",
			Self::Json => "jsonb",
			Self::Uuid => "uuid",
			Self::Char => "char",
			Self::Name => "name",
			Self::Varchar => "varchar",
			Self::Bpchar => "bpchar",
			Self::Timetz => "timetz",
			Self::Inet => "inet",
			Self::Cidr => "cidr",
			Self::MacAddr => "macaddr",
			Self::Oid => "oid",
			Self::Regclass => "regclass",
			Self::Record => "record",
			Self::Int2Vector => "int2vector",
			Self::OidVector => "oidvector",
			Self::Vector => "vector",
		};
		Some(name.to_owned())
	}

	/// Parse SQL type from a string name (e.g. "int4", "varchar", "boolean", "text", etc.). A name
	/// followed by `[]` is an array of that type.
	pub fn from_name(name: &str) -> Self {
		let lower = name.to_ascii_lowercase();
		// `vector(3)` is a vector: its dimensions do not change how it travels.
		let lower = match lower.strip_prefix("vector(") {
			Some(_) => "vector".to_string(),
			None => lower,
		};
		if let Some(elem) = lower.strip_suffix("[]") {
			return Self::array_of(Self::from_name(elem)).unwrap_or(Self::Unknown);
		}
		match lower.as_str() {
			"bool" | "boolean" => Self::Bool,
			"int2" | "smallint" => Self::Int2,
			"int4" | "int" | "integer" => Self::Int4,
			"int8" | "bigint" => Self::Int8,
			"float4" | "real" => Self::Float4,
			"float8" | "double" | "double precision" | "float" => Self::Float8,
			"numeric" | "decimal" => Self::Numeric,
			"text" | "string" => Self::Text,
			"name" => Self::Name,
			"varchar" | "character varying" => Self::Varchar,
			"bpchar" | "char" | "character" => Self::Bpchar,
			"\"char\"" => Self::Char,
			"bytea" | "blob" | "bytes" => Self::Bytea,
			"date" => Self::Date,
			"time" | "time without time zone" => Self::Time,
			"timetz" | "time with time zone" => Self::Timetz,
			"timestamp" | "timestamp without time zone" => Self::Timestamp,
			"timestamptz" | "timestamp with time zone" => Self::TimestampTz,
			"interval" => Self::Interval,
			"uuid" => Self::Uuid,
			"json" | "jsonb" => Self::Json,
			"array" => Self::Array(&Self::Text),
			"inet" => Self::Inet,
			"cidr" => Self::Cidr,
			"macaddr" => Self::MacAddr,
			"oid" => Self::Oid,
			"regclass" => Self::Regclass,
			"record" => Self::Record,
			"int2vector" => Self::Int2Vector,
			"oidvector" => Self::OidVector,
			"vector" => Self::Vector,
			_ => Self::Unknown,
		}
	}
}

/// Description of a single projected output column. A statement that is not a select describes
/// the columns of its result by their name and type and leaves every other field at its default.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResultColumn {
	/// Wire name of column (alias, column name, or ?column? in PG / expr in MySQL).
	pub name: String,
	/// Inferred SQL data type.
	pub ty: SqlType,
	/// Source table if resolved.
	pub table: Option<String>,
	/// Unqualified original column name if resolved.
	pub original_column: Option<String>,
	/// The field of each row of the plan of [`lower_bound_select`](crate::lower_bound_select) that
	/// holds this column's value, which is not the name a client is shown: two columns may share a
	/// name and a column may have none. It is distinct among the columns of one result. `None`
	/// where the rows are read by name: the select has a `*` or `t.*` the planner cannot list, so
	/// its columns are known only from its rows, or the rows are not those of a select.
	pub key: Option<String>,
	/// Whether the column is a `*` or `t.*` whose columns are known only from the rows that come
	/// back, as opposed to a column that merely has the name `*`.
	pub dynamic: bool,
}

/// Descriptor of a query result set produced at plan time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultDescriptor {
	/// Ordered projected columns.
	pub columns: Vec<ResultColumn>,
}

impl ResultDescriptor {
	/// Whether a column is `*` or `t.*` over a relation whose columns the binder could not list:
	/// the result's columns are known only from the rows that come back. A column that is aliased
	/// `*` is a column like any other.
	pub fn has_dynamic_columns(&self) -> bool {
		self.columns.iter().any(|column| column.dynamic)
	}
}

/// Descriptor of statement parameters ($1..$N or ?).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterDescriptor {
	/// Types for parameters 1..N.
	pub param_types: Vec<SqlType>,
}

/// Descriptor of a prepared statement before execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementDescriptor {
	/// Statement returns a result set with known columns.
	Rows(ResultDescriptor),
	/// Non-returning statement (e.g. DML/DDL without RETURNING) yielding NoData reply.
	NoData {
		tag: &'static str,
	},
}

impl BoundSelect {
	/// Whether a projection is `*` or `t.*` over a relation whose columns the
	/// binder cannot enumerate, so the result's columns are known only from the
	/// rows that come back.
	pub fn has_dynamic_columns(&self) -> bool {
		self.targets.iter().any(BoundTarget::is_unlisted_wildcard)
	}

	/// The result descriptor, or `None` when the columns are not known before
	/// the statement runs (see [`Self::has_dynamic_columns`]). A caller holding
	/// `None` shapes the result from its rows.
	pub fn describe_static(
		&self,
		catalog: &dyn BinderCatalog,
		dialect: &DialectPolicy,
	) -> Option<ResultDescriptor> {
		if self.has_dynamic_columns() {
			None
		} else {
			Some(self.describe(catalog, dialect))
		}
	}

	/// Compute the result descriptor for this bound SELECT query.
	pub fn describe(
		&self,
		catalog: &dyn BinderCatalog,
		dialect: &DialectPolicy,
	) -> ResultDescriptor {
		let mut columns = Vec::with_capacity(self.targets.len());
		let mut keys = output_keys(&self.targets).map(Vec::into_iter);
		for target in &self.targets {
			let key = keys.as_mut().and_then(Iterator::next);
			if target.is_wildcard {
				columns.push(ResultColumn {
					name: "*".to_string(),
					ty: SqlType::Unknown,
					table: None,
					original_column: None,
					key,
					dynamic: true,
				});
				continue;
			}
			let (name, table, orig_col) = match &target.alias {
				Some(alias) => (alias.clone(), None, None),
				None => match &target.expr {
					BoundExpr::Column(col) => (
						col.column.clone(),
						Some(col.relation_name.clone()),
						Some(col.column.clone()),
					),
					BoundExpr::FunctionCall {
						name,
						..
					}
					| BoundExpr::WindowFunction {
						name,
						..
					} => (name.clone(), None, None),
					BoundExpr::UserVar(name) => (format!("@{name}"), None, None),
					BoundExpr::SystemVar {
						name,
						is_global,
					} => (
						if *is_global {
							format!("@@global.{name}")
						} else {
							format!("@@{name}")
						},
						None,
						None,
					),
					BoundExpr::Literal(_)
					| BoundExpr::Access {
						..
					}
					| BoundExpr::Param {
						..
					}
					| BoundExpr::Binary {
						..
					}
					| BoundExpr::Unary {
						..
					}
					| BoundExpr::Case {
						..
					}
					| BoundExpr::Between {
						..
					}
					| BoundExpr::InList {
						..
					}
					| BoundExpr::IsNull {
						..
					}
					| BoundExpr::Cast {
						..
					}
					| BoundExpr::ScalarSubquery(_)
					| BoundExpr::Exists(_)
					| BoundExpr::InSubquery {
						..
					}
					| BoundExpr::ArraySubquery(_) => {
						if dialect.dialect == SqlDialect::Postgres {
							("?column?".to_string(), None, None)
						} else {
							("?".to_string(), None, None)
						}
					}
				},
			};
			let mut ty = self.infer_expr_type(&target.expr, catalog);
			if dialect.dialect == SqlDialect::Postgres && is_case_of_integer_constants(&target.expr)
			{
				ty = SqlType::Int4;
			}
			let dynamic = target.alias.is_none() && target.is_unlisted_wildcard();
			columns.push(ResultColumn {
				name,
				ty,
				table,
				original_column: orig_col,
				key,
				dynamic,
			});
		}
		ResultDescriptor {
			columns,
		}
	}

	/// Infer the SQL type of an expression in the context of this SELECT.
	pub fn infer_expr_type(&self, expr: &BoundExpr, catalog: &dyn BinderCatalog) -> SqlType {
		match expr {
			BoundExpr::Literal(lit) => match lit {
				BoundLiteral::Integer(_) => SqlType::Int8,
				BoundLiteral::Float(_) => SqlType::Float8,
				BoundLiteral::String(_) => SqlType::Text,
				BoundLiteral::Boolean(_) => SqlType::Bool,
				BoundLiteral::Null => SqlType::Unknown,
			},
			BoundExpr::Column(col) => {
				if col.declared_type != SqlType::Unknown {
					return col.declared_type;
				}
				if !col.relation_name.is_empty() {
					let dt = catalog.get_table(None, &col.relation_name).and_then(|tbl| {
						tbl.find_column(&col.column, true).and_then(|c| c.data_type.clone())
					});
					if let Some(dt) = dt {
						return SqlType::from_name(&dt);
					}
				}
				SqlType::Unknown
			}
			BoundExpr::Cast {
				target_type,
				..
			} => SqlType::from_name(target_type),
			BoundExpr::Binary {
				left,
				op,
				right,
			} => match op {
				BoundBinaryOp::Equal
				| BoundBinaryOp::NotEqual
				| BoundBinaryOp::LessThan
				| BoundBinaryOp::LessThanEqual
				| BoundBinaryOp::GreaterThan
				| BoundBinaryOp::GreaterThanEqual
				| BoundBinaryOp::And
				| BoundBinaryOp::Or
				| BoundBinaryOp::NullSafeEqual
				| BoundBinaryOp::DistinctFrom
				| BoundBinaryOp::Like
				| BoundBinaryOp::ILike
				| BoundBinaryOp::Regexp
				| BoundBinaryOp::RegexpCi => SqlType::Bool,
				BoundBinaryOp::Power => SqlType::Float8,
				BoundBinaryOp::Concat => SqlType::Text,
				BoundBinaryOp::IntDiv => SqlType::Int8,
				BoundBinaryOp::JsonExtract => SqlType::Json,
				BoundBinaryOp::JsonExtractText | BoundBinaryOp::JsonPathExtractText => {
					SqlType::Text
				}
				BoundBinaryOp::JsonContains => SqlType::Bool,
				BoundBinaryOp::VectorDistance(_) => SqlType::Float8,
				BoundBinaryOp::Add
				| BoundBinaryOp::Subtract
				| BoundBinaryOp::Multiply
				| BoundBinaryOp::Divide
				| BoundBinaryOp::Remainder => {
					let lt = self.infer_expr_type(left, catalog);
					let rt = self.infer_expr_type(right, catalog);
					if lt == SqlType::Float8
						|| rt == SqlType::Float8
						|| lt == SqlType::Float4
						|| rt == SqlType::Float4
					{
						SqlType::Float8
					} else if lt == SqlType::Numeric || rt == SqlType::Numeric {
						SqlType::Numeric
					} else if lt == SqlType::Int8 || rt == SqlType::Int8 {
						SqlType::Int8
					} else if lt == SqlType::Int4 || rt == SqlType::Int4 {
						SqlType::Int4
					} else if lt == SqlType::Int2 || rt == SqlType::Int2 {
						SqlType::Int2
					} else {
						SqlType::Int8
					}
				}
			},
			BoundExpr::Unary {
				op,
				expr,
			} => match op {
				BoundUnaryOp::Not => SqlType::Bool,
				BoundUnaryOp::Positive | BoundUnaryOp::Negate => {
					self.infer_expr_type(expr, catalog)
				}
			},
			BoundExpr::IsNull {
				..
			}
			| BoundExpr::Between {
				..
			}
			| BoundExpr::InList {
				..
			} => SqlType::Bool,
			BoundExpr::FunctionCall {
				name,
				args,
				..
			} => {
				let arg_types: Vec<Option<SqlType>> = args
					.iter()
					.map(|a| (!is_null_literal(a)).then(|| self.infer_expr_type(a, catalog)))
					.collect();
				FunctionRegistry::infer_return_type_of_arguments(name, &arg_types)
			}
			BoundExpr::WindowFunction {
				name,
				args,
				..
			} => {
				let arg_types: Vec<_> =
					args.iter().map(|a| self.infer_expr_type(a, catalog)).collect();
				FunctionRegistry::infer_return_type_any(name, &arg_types)
			}
			BoundExpr::Param {
				..
			} => SqlType::Unknown,
			BoundExpr::Access {
				..
			} => SqlType::Unknown,
			BoundExpr::Case {
				branches,
				else_expr,
				..
			} => {
				// A NULL branch has no value of its own, so it does not take part in the type of
				// the others.
				SqlType::common(
					branches
						.iter()
						.map(|(_, then_expr)| then_expr)
						.chain(else_expr.as_deref())
						.filter(|branch| !is_null_literal(branch))
						.map(|branch| self.infer_expr_type(branch, catalog)),
				)
			}
			BoundExpr::ScalarSubquery(sub) => {
				if let Some(target) = sub.targets.first() {
					self.infer_expr_type(&target.expr, catalog)
				} else {
					SqlType::Unknown
				}
			}
			BoundExpr::Exists(_) => SqlType::Bool,
			BoundExpr::InSubquery {
				..
			} => SqlType::Bool,
			BoundExpr::ArraySubquery(_) => SqlType::Unknown,
			BoundExpr::UserVar(_) => SqlType::Unknown,
			BoundExpr::SystemVar {
				name,
				..
			} => match crate::system_vars::mysql_system_variable(name) {
				Some(
					crate::system_vars::SystemVariable::Int(_)
					| crate::system_vars::SystemVariable::Flag(_),
				) => SqlType::Int8,
				Some(crate::system_vars::SystemVariable::Text(_)) | None => SqlType::Text,
			},
		}
	}

	/// Infer parameter types based on their usage in expressions and predicates.
	pub fn describe_parameters(&self, catalog: &dyn BinderCatalog) -> ParameterDescriptor {
		let mut param_map = HashMap::new();

		if let Some(where_clause) = &self.where_clause {
			self.collect_param_types_from_expr(where_clause, catalog, &mut param_map);
		}

		for target in &self.targets {
			self.collect_param_types_from_expr(&target.expr, catalog, &mut param_map);
		}

		for order in &self.order_by {
			self.collect_param_types_from_expr(&order.expr, catalog, &mut param_map);
		}

		if let Some(with) = &self.with {
			for cte in &with.ctes {
				if let Some(where_clause) = &cte.query.where_clause {
					cte.query.collect_param_types_from_expr(where_clause, catalog, &mut param_map);
				}
				for target in &cte.query.targets {
					cte.query.collect_param_types_from_expr(&target.expr, catalog, &mut param_map);
				}
			}
		}

		for win in &self.windows {
			for p in &win.spec.partition_by {
				self.collect_param_types_from_expr(p, catalog, &mut param_map);
			}
			for o in &win.spec.order_by {
				self.collect_param_types_from_expr(&o.expr, catalog, &mut param_map);
			}
		}

		for expression in self.limit.iter().chain(self.offset.iter()) {
			self.collect_param_types_from_expr(expression, catalog, &mut param_map);
			if let BoundExpr::Param {
				index,
				..
			} = expression
			{
				let ty = param_map.entry(*index).or_insert(SqlType::Int8);
				if *ty == SqlType::Unknown {
					*ty = SqlType::Int8;
				}
			}
		}

		if let Some(source) = &self.from {
			self.collect_source_parameters(source, catalog, &mut param_map);
		}

		let max_idx = param_map.keys().copied().max().unwrap_or(0);
		let mut param_types = Vec::with_capacity(max_idx);
		for idx in 1..=max_idx {
			param_types.push(param_map.get(&idx).copied().unwrap_or(SqlType::Unknown));
		}

		ParameterDescriptor {
			param_types,
		}
	}

	fn collect_source_parameters(
		&self,
		source: &BoundTableSource,
		catalog: &dyn BinderCatalog,
		params: &mut HashMap<usize, SqlType>,
	) {
		match source {
			BoundTableSource::Subquery {
				query,
				..
			} => {
				for (index, ty) in
					query.describe_parameters(catalog).param_types.into_iter().enumerate()
				{
					let existing = params.entry(index + 1).or_insert(ty);
					if *existing == SqlType::Unknown {
						*existing = ty;
					}
				}
			}
			BoundTableSource::Join {
				left,
				right,
				..
			} => {
				self.collect_source_parameters(left, catalog, params);
				self.collect_source_parameters(right, catalog, params);
			}
			BoundTableSource::Lateral {
				targets,
				..
			} => {
				for target in targets {
					self.collect_param_types_from_expr(&target.expr, catalog, params);
				}
			}
			BoundTableSource::Table {
				..
			}
			| BoundTableSource::Values => {}
		}
	}

	fn collect_param_types_from_expr(
		&self,
		expr: &BoundExpr,
		catalog: &dyn BinderCatalog,
		params: &mut HashMap<usize, SqlType>,
	) {
		match expr {
			BoundExpr::Binary {
				left,
				op,
				right,
			} => {
				// A parameter of a distance is the vector it is the distance to.
				if let BoundBinaryOp::VectorDistance(_) = op {
					for operand in [left, right] {
						if let BoundExpr::Param {
							index,
							..
						} = &**operand
						{
							params.insert(*index, SqlType::Vector);
						}
					}
				}
				match (&**left, &**right) {
					(
						BoundExpr::Param {
							index,
							..
						},
						other,
					)
					| (
						other,
						BoundExpr::Param {
							index,
							..
						},
					) => {
						let ty = self.infer_expr_type(other, catalog);
						if ty != SqlType::Unknown {
							params
								.entry(*index)
								.and_modify(|existing| {
									if *existing == SqlType::Unknown {
										*existing = ty;
									}
								})
								.or_insert(ty);
						}
					}
					_ => {}
				}
				self.collect_param_types_from_expr(left, catalog, params);
				self.collect_param_types_from_expr(right, catalog, params);
			}
			BoundExpr::Cast {
				expr,
				target_type,
			} => {
				if let BoundExpr::Param {
					index,
					..
				} = &**expr
				{
					let ty = SqlType::from_name(target_type);
					if ty != SqlType::Unknown {
						params.insert(*index, ty);
					}
				}
				self.collect_param_types_from_expr(expr, catalog, params);
			}
			BoundExpr::Unary {
				expr,
				..
			}
			| BoundExpr::IsNull {
				expr,
				..
			} => {
				self.collect_param_types_from_expr(expr, catalog, params);
			}
			BoundExpr::FunctionCall {
				name,
				args,
				..
			} => {
				// `x op ANY/ALL ($n)`: the parameter is an array of x's type. An operand whose
				// type the catalog cannot state is presented as text everywhere else, so its
				// array is `text[]`.
				if name == crate::functions::QUANTIFIED_ARRAY
					&& let [
						lhs,
						BoundExpr::Param {
							index,
							..
						},
						..,
					] = args.as_slice()
					&& let element = self.infer_expr_type(lhs, catalog)
					&& let Some(array_ty) = SqlType::array_of(if element == SqlType::Unknown {
						SqlType::Text
					} else {
						element
					}) {
					params
						.entry(*index)
						.and_modify(|existing| {
							if *existing == SqlType::Unknown {
								*existing = array_ty;
							}
						})
						.or_insert(array_ty);
				}
				// A parameter handed to a vector function is the vector it takes.
				if crate::vector::is_distance_function(name) || name == "inner_product" {
					for arg in args {
						if let BoundExpr::Param {
							index,
							..
						} = arg
						{
							params.insert(*index, SqlType::Vector);
						}
					}
				}
				for arg in args {
					self.collect_param_types_from_expr(arg, catalog, params);
				}
			}
			BoundExpr::Case {
				operand,
				branches,
				else_expr,
			} => {
				if let Some(op) = operand {
					self.collect_param_types_from_expr(op, catalog, params);
				}
				for (cond, val) in branches {
					self.collect_param_types_from_expr(cond, catalog, params);
					self.collect_param_types_from_expr(val, catalog, params);
				}
				if let Some(el) = else_expr {
					self.collect_param_types_from_expr(el, catalog, params);
				}
			}
			BoundExpr::Between {
				expr,
				low,
				high,
				..
			} => {
				self.collect_param_types_from_expr(expr, catalog, params);
				self.collect_param_types_from_expr(low, catalog, params);
				self.collect_param_types_from_expr(high, catalog, params);
			}
			BoundExpr::InList {
				expr,
				list,
				..
			} => {
				self.collect_param_types_from_expr(expr, catalog, params);
				for item in list {
					self.collect_param_types_from_expr(item, catalog, params);
				}
			}
			BoundExpr::InSubquery {
				expr,
				..
			} => {
				self.collect_param_types_from_expr(expr, catalog, params);
			}
			BoundExpr::WindowFunction {
				args,
				spec,
				..
			} => {
				for arg in args {
					self.collect_param_types_from_expr(arg, catalog, params);
				}
				for p in &spec.partition_by {
					self.collect_param_types_from_expr(p, catalog, params);
				}
				for o in &spec.order_by {
					self.collect_param_types_from_expr(&o.expr, catalog, params);
				}
				if let Some(frame) = &spec.frame {
					if let BoundWindowFrameBound::Preceding(e)
					| BoundWindowFrameBound::Following(e) = &frame.start
					{
						self.collect_param_types_from_expr(e, catalog, params);
					}
					if let Some(
						BoundWindowFrameBound::Preceding(e) | BoundWindowFrameBound::Following(e),
					) = &frame.end
					{
						self.collect_param_types_from_expr(e, catalog, params);
					}
				}
			}
			BoundExpr::Param {
				index,
				..
			} => {
				params.entry(*index).or_insert(SqlType::Unknown);
			}
			BoundExpr::Literal(_)
			| BoundExpr::Column(_)
			| BoundExpr::Access {
				..
			}
			| BoundExpr::ScalarSubquery(_)
			| BoundExpr::Exists(_)
			| BoundExpr::ArraySubquery(_)
			| BoundExpr::UserVar(_)
			| BoundExpr::SystemVar {
				..
			} => {}
		}
	}
}

/// Whether `expr` is a CASE whose every result is an integer constant that fits `integer`, which
/// PostgreSQL types as `integer`, as it types the constants.
fn is_case_of_integer_constants(expr: &BoundExpr) -> bool {
	let BoundExpr::Case {
		branches,
		else_expr,
		..
	} = expr
	else {
		return false;
	};
	let fits_int4 = |expr: &BoundExpr| matches!(expr, BoundExpr::Literal(BoundLiteral::Integer(n)) if i32::try_from(*n).is_ok());
	!branches.is_empty()
		&& branches.iter().all(|(_, then)| fits_int4(then))
		&& else_expr.as_deref().is_none_or(fits_int4)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::EmptyCatalog;

	#[test]
	fn a_type_name_reads_back_as_the_same_type() {
		let scalars = [
			SqlType::Bool,
			SqlType::Int2,
			SqlType::Int4,
			SqlType::Int8,
			SqlType::Float4,
			SqlType::Float8,
			SqlType::Numeric,
			SqlType::Text,
			SqlType::Bytea,
			SqlType::Date,
			SqlType::Time,
			SqlType::Timestamp,
			SqlType::TimestampTz,
			SqlType::Interval,
			SqlType::Json,
			SqlType::Uuid,
			SqlType::Char,
			SqlType::Name,
			SqlType::Varchar,
			SqlType::Bpchar,
			SqlType::Timetz,
			SqlType::Inet,
			SqlType::Cidr,
			SqlType::MacAddr,
			SqlType::Oid,
			SqlType::Regclass,
			SqlType::Record,
		];
		for ty in scalars {
			let name = ty.name().expect("a scalar has a name");
			assert_eq!(SqlType::from_name(&name), ty, "{name}");
			let array = SqlType::array_of(ty).expect("a scalar has an array");
			let name = array.name().expect("an array has a name");
			assert_eq!(SqlType::from_name(&name), array, "{name}");
		}
		assert_eq!(SqlType::Unknown.name(), None);
		assert_eq!(SqlType::Int8.name().as_deref(), Some("bigint"));
		assert_eq!(
			SqlType::array_of(SqlType::Text).and_then(|t| t.name()).as_deref(),
			Some("text[]")
		);
	}

	fn pg(sql: &str) -> (BoundSelect, DialectPolicy) {
		let stmts = surrealdb_pgsql::parse_statements(sql).expect("parse");
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let policy = DialectPolicy::postgres();
		(
			crate::binder::pgsql::bind_select(select, &EmptyCatalog, &policy, None).expect("bind"),
			policy,
		)
	}

	fn my(sql: &str) -> (BoundSelect, DialectPolicy) {
		let stmts = surrealdb_mysql::parse_statements(sql).expect("parse");
		let surrealdb_mysql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("expected SELECT");
		};
		let policy = DialectPolicy::mysql();
		(
			crate::binder::mysql::bind_select(select, &EmptyCatalog, &policy, None).expect("bind"),
			policy,
		)
	}

	#[test]
	fn a_wildcard_is_known_only_from_the_rows() {
		for (bound, policy) in [
			pg("SELECT * FROM person"),
			pg("SELECT *, age FROM person"),
			my("SELECT * FROM person"),
			my("SELECT p.* FROM person p"),
		] {
			assert!(bound.has_dynamic_columns());
			assert!(bound.describe_static(&EmptyCatalog, &policy).is_none());
			assert!(bound.describe(&EmptyCatalog, &policy).has_dynamic_columns());
		}
	}

	#[test]
	fn a_column_named_star_is_a_column_like_any_other() {
		for (bound, policy) in [my("SELECT age, `*` FROM person"), my("SELECT p.`*` FROM person p")]
		{
			assert!(!bound.has_dynamic_columns());
			let descriptor = bound.describe_static(&EmptyCatalog, &policy).expect("static");
			assert!(!descriptor.has_dynamic_columns());
			assert_eq!(descriptor.columns.last().map(|c| c.name.as_str()), Some("*"));
		}
	}

	#[test]
	fn a_column_aliased_star_is_a_column_like_any_other() {
		for (bound, policy) in
			[pg("SELECT age, name AS \"*\" FROM person"), my("SELECT age, name AS `*` FROM person")]
		{
			assert!(!bound.has_dynamic_columns());
			let descriptor = bound.describe_static(&EmptyCatalog, &policy).expect("static");
			assert!(!descriptor.has_dynamic_columns());
			let names: Vec<&str> = descriptor.columns.iter().map(|c| c.name.as_str()).collect();
			assert_eq!(names, ["age", "*"]);
		}
	}
}

#[cfg(test)]
mod declared_type_tests {
	use super::*;
	use crate::catalog::{BinderCatalog, MemoryCatalog};

	#[test]
	fn scalar_type_hints_survive_aliases_without_enumerating_columns() {
		let catalog = MemoryCatalog::new()
			.with_column_type("people", "age", SqlType::Int8)
			.with_column_type("people", "enabled", SqlType::Bool);
		assert!(catalog.get_table(None, "people").is_none());
		let stmts = surrealdb_pgsql::parse_statements(
			"SELECT p.age, p.enabled FROM people p WHERE p.age = $2 AND $1 IS NULL",
		)
		.unwrap();
		let surrealdb_pgsql::ast::stmt::Statement::Select(select) = &stmts[0] else {
			panic!("SELECT");
		};
		let policy = DialectPolicy::postgres();
		let bound = crate::binder::pgsql::bind_select(select, &catalog, &policy, None).unwrap();
		let desc = bound.describe(&catalog, &policy);
		assert_eq!(
			desc.columns.iter().map(|col| col.ty).collect::<Vec<_>>(),
			vec![SqlType::Int8, SqlType::Bool]
		);
		assert_eq!(
			bound.describe_parameters(&catalog).param_types,
			vec![SqlType::Unknown, SqlType::Int8]
		);
	}
}
