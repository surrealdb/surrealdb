//! Result descriptors, static typing, and parameter description for SQL statements.

use std::collections::HashMap;

use crate::catalog::BinderCatalog;
use crate::dialect::{DialectPolicy, SqlDialect};
use crate::functions::FunctionRegistry;
use crate::ir::{
	BoundBinaryOp, BoundExpr, BoundLiteral, BoundSelect, BoundUnaryOp, BoundWindowFrameBound,
};

/// Abstract strongly-typed SQL data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlType {
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
	Array,
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
}

impl SqlType {
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
			Self::Array => 2277,
			Self::Uuid => 2950,
		}
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
			| Self::Array
			| Self::Inet
			| Self::Cidr
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
			Self::Json | Self::Array | Self::Record => 0xf5, // MYSQL_TYPE_JSON
		}
	}

	/// Parse SQL type from a string name (e.g. "int4", "varchar", "boolean", "text", etc.)
	pub fn from_name(name: &str) -> Self {
		let lower = name.to_ascii_lowercase();
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
			"array" => Self::Array,
			"inet" => Self::Inet,
			"cidr" => Self::Cidr,
			"macaddr" => Self::MacAddr,
			"oid" => Self::Oid,
			"regclass" => Self::Regclass,
			"record" => Self::Record,
			_ => Self::Unknown,
		}
	}
}

/// Description of a single projected output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumn {
	/// Wire name of column (alias, column name, or ?column? in PG / expr in MySQL).
	pub name: String,
	/// Inferred SQL data type.
	pub ty: SqlType,
	/// Source table if resolved.
	pub table: Option<String>,
	/// Unqualified original column name if resolved.
	pub original_column: Option<String>,
}

/// Descriptor of a query result set produced at plan time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultDescriptor {
	/// Ordered projected columns.
	pub columns: Vec<ResultColumn>,
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
	/// Compute the result descriptor for this bound SELECT query.
	pub fn describe(
		&self,
		catalog: &dyn BinderCatalog,
		dialect: &DialectPolicy,
	) -> ResultDescriptor {
		let mut columns = Vec::with_capacity(self.targets.len());
		for target in &self.targets {
			if target.is_wildcard {
				columns.push(ResultColumn {
					name: "*".to_string(),
					ty: SqlType::Unknown,
					table: None,
					original_column: None,
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
					_ => {
						if dialect.dialect == SqlDialect::Postgres {
							("?column?".to_string(), None, None)
						} else {
							("?".to_string(), None, None)
						}
					}
				},
			};
			let ty = self.infer_expr_type(&target.expr, catalog);
			columns.push(ResultColumn {
				name,
				ty,
				table,
				original_column: orig_col,
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
				| BoundBinaryOp::Regexp => SqlType::Bool,
				BoundBinaryOp::Concat => SqlType::Text,
				BoundBinaryOp::IntDiv => SqlType::Int8,
				BoundBinaryOp::JsonExtract => SqlType::Json,
				BoundBinaryOp::JsonExtractText | BoundBinaryOp::JsonPathExtractText => {
					SqlType::Text
				}
				BoundBinaryOp::JsonContains => SqlType::Bool,
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
				let arg_types: Vec<_> =
					args.iter().map(|a| self.infer_expr_type(a, catalog)).collect();
				FunctionRegistry::infer_return_type_any(name, &arg_types)
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
				if let Some((_, then_expr)) = branches.first() {
					self.infer_expr_type(then_expr, catalog)
				} else if let Some(el) = else_expr {
					self.infer_expr_type(el, catalog)
				} else {
					SqlType::Unknown
				}
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
			} => match name.to_ascii_lowercase().as_str() {
				"autocommit"
				| "max_allowed_packet"
				| "interactive_timeout"
				| "wait_timeout"
				| "net_write_timeout"
				| "net_read_timeout"
				| "lower_case_table_names"
				| "auto_increment_increment"
				| "auto_increment_offset"
				| "query_cache_size"
				| "query_cache_type"
				| "tx_read_only"
				| "transaction_read_only" => SqlType::Int8,
				_ => SqlType::Text,
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

		let max_idx = param_map.keys().copied().max().unwrap_or(0);
		let mut param_types = Vec::with_capacity(max_idx);
		for idx in 1..=max_idx {
			param_types.push(param_map.get(&idx).copied().unwrap_or(SqlType::Unknown));
		}

		ParameterDescriptor {
			param_types,
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
				right,
				..
			} => {
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
							params.entry(*index).or_insert(ty);
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
				args,
				..
			} => {
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
					match &frame.start {
						BoundWindowFrameBound::Preceding(e)
						| BoundWindowFrameBound::Following(e) => {
							self.collect_param_types_from_expr(e, catalog, params);
						}
						_ => {}
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
			_ => {}
		}
	}
}
