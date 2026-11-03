//! Lowering from BoundExpr to SurrealDB `LogicalExpr`.

use surrealdb_expr::expr::logical::{
	LogicalCall, LogicalExpr, LogicalField, LogicalIf, LogicalLiteral, LogicalOrder, OutputPath,
	RelationAlias,
};
use surrealdb_expr::expr::{BinaryOperator, Function, Kind, Param, PrefixOperator};
use surrealdb_strand::Strand;

use crate::SqlType;
use crate::array_literal::{is_array_space, parse_text_array_literal};
use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::functions::FunctionRegistry;
use crate::ir::{
	BoundBinaryOp, BoundColumnRef, BoundExpr, BoundLiteral, BoundOrder, BoundOrderDirection,
	BoundTarget, BoundUnaryOp,
};
use crate::lower::enum_rank::{extreme, is_label_set, label_set_literal, rank, scalar_enum};
use crate::lower::id_key::{
	equal, id_column, is_key_or_link, key_of, key_operand, key_read_as, lower_id_key, record_of,
	record_of_operand_typed, record_or_value,
};
use crate::lower::link::{
	LinkOperand, link_column, link_comparison_operand, link_operand, lower_stored, same_table_pair,
};
use crate::lower::network_rank::{
	canonical_literal as canonical_network_literal, canonical_param as canonical_network_param,
	common_network, is_network, key as network_key,
};
use crate::lower::plan::lower_bound_subquery;
use crate::lower::quote_ident::{lower_quote_ident, quote_ident_str};
use crate::lower::sql_like::like_pattern_regex;
use crate::semantics::sql_like_to_regex;
use crate::system_vars::{MYSQL_SERVER_VERSION, SystemVariable, mysql_system_variable};

/// The engine kind a cast to the SQL type produces, `Kind::Any` for a type the cast leaves as it
/// is.
pub(crate) fn cast_kind(target_type: &str, dialect: SqlDialect) -> Kind {
	// A type the binder resolved to an enum of the database, with one `[]` for each array level.
	if let Some(spec) = target_type.strip_prefix("enum:") {
		let mut name = spec;
		let mut levels = 0;
		while let Some(inner) = name.strip_suffix("[]") {
			name = inner;
			levels += 1;
		}
		let mut kind = Kind::Named(name.to_owned());
		for _ in 0..levels {
			kind = Kind::Array(Box::new(kind), None);
		}
		return kind;
	}
	let clean_type = target_type.to_lowercase();
	let base_type = clean_type.split('(').next().unwrap_or(&clean_type).trim();
	match base_type {
		"int" | "integer" | "int4" | "int8" | "int2" | "smallint" | "bigint" | "signed" => {
			Kind::Int
		}
		"unsigned" => Kind::U64,
		"float" | "float8" | "float4" | "double" | "real" | "double precision" => Kind::Float,
		"numeric" | "decimal" | "dec" => {
			// MySQL `DECIMAL` without a precision is `DECIMAL(10,0)`; PostgreSQL `numeric` without
			// one holds any number of digits.
			let precision = clean_type
				.split_once('(')
				.map(|(_, rest)| rest.split([',', ')']).next().unwrap_or("").trim());
			let precision = match (precision, dialect) {
				(Some(precision), _) => Some(precision),
				(None, SqlDialect::MySql) => Some("10"),
				(None, SqlDialect::Postgres) => None,
			};
			crate::lower::ddl::decimal_kind(precision)
		}
		"money" => Kind::Decimal,
		// The catalog names a relation, a type or a function by its name, which stands for its
		// OID: a cast to `oid` or a `reg*` type keeps its operand, so a name stays the name a
		// client reads back and a number stays the OID it is.
		"oid" | "regclass" | "regproc" | "regprocedure" | "regoper" | "regoperator" | "regtype"
		| "regnamespace" | "regrole" | "regconfig" | "regdictionary" | "regcollation" => Kind::Any,
		"xid" | "cid" => Kind::U32,
		"xid8" | "pg_lsn" => Kind::U64,
		"text" | "varchar" | "char" | "character varying" | "bpchar" | "string" => Kind::String,
		"bool" | "boolean" => Kind::Bool,
		"date" => Kind::Date,
		// MySQL `TIME` is a signed duration, which is an interval; PostgreSQL `TIME` and `TIMETZ`
		// are a time of day.
		"time" if dialect == SqlDialect::MySql => Kind::Interval,
		"time" | "timetz" | "time without time zone" | "time with time zone" => Kind::Time,
		"interval" => Kind::Interval,
		// A PostgreSQL interval with a field qualifier, such as `interval day to second`.
		base if dialect == SqlDialect::Postgres && base.starts_with("interval ") => Kind::Interval,
		// MySQL `DATETIME` has no zone and `TIMESTAMP` is an instant, as PostgreSQL's
		// `timestamp` and `timestamptz` are.
		"datetime" | "timestamp" | "timestamp without time zone"
			if dialect == SqlDialect::Postgres || base_type == "datetime" =>
		{
			Kind::Timestamp
		}
		"timestamp" | "timestamptz" | "timestamp with time zone" => Kind::Datetime,
		_ => Kind::Any,
	}
}

/// The value of a numeric constant: a decimal when it fits one, and a bigdecimal otherwise. Both
/// are normalised, as a SurrealQL decimal literal is, so `1.50` reads as `1.5`.
pub(crate) enum NumericValue {
	Decimal(surrealdb_types::Decimal),
	BigDecimal(surrealdb_types::BigDecimal),
}

/// Read the text of a numeric constant (`1.50`, `1e3`, `123456789012345678901234567890.5`).
pub(crate) fn numeric_value(text: &str) -> Result<NumericValue, PlanError> {
	let big = text
		.parse::<surrealdb_types::BigDecimal>()
		.map_err(|_| PlanError::InvalidTextRepresentation(format!("numeric: \"{text}\"")))?;
	Ok(match surrealdb_types::Decimal::try_from(&big) {
		Ok(decimal) => NumericValue::Decimal(decimal),
		Err(_) => NumericValue::BigDecimal(big),
	})
}

/// The declared type of a cast to `numeric(p,s)` or `decimal(p,s)`, normalised to
/// `numeric(p,s)`. A MySQL `DECIMAL` without a precision is `DECIMAL(10,0)`. `None` for a
/// PostgreSQL `numeric` without a precision, which holds any number of digits, and for every
/// other type.
fn scaled_numeric(clean_type: &str, dialect: SqlDialect) -> Option<String> {
	let is_numeric = |base: &str| matches!(base.trim(), "numeric" | "decimal" | "dec" | "fixed");
	let Some((base, modifiers)) = clean_type.split_once('(') else {
		return (dialect == SqlDialect::MySql && is_numeric(clean_type))
			.then(|| "numeric(10,0)".to_owned());
	};
	if !is_numeric(base) {
		return None;
	}
	let modifiers: String = modifiers.chars().filter(|c| !c.is_whitespace()).collect();
	Some(format!("numeric({modifiers}"))
}

/// Whether a cast to text of `expr` is SurrealQL's cast to a string: for a string literal, an
/// operand of a text type, and the key an `id` or link column reads.
fn is_text_operand(expr: &BoundExpr, dialect: SqlDialect) -> bool {
	if let BoundExpr::Literal(BoundLiteral::String(_)) = expr {
		return true;
	}
	// The key of an `id` or link column is cast as SurrealQL casts a key.
	if let BoundExpr::Column(col) = expr
		&& (col.id_table.is_some() || col.link_table.is_some())
	{
		return true;
	}
	let ty = crate::lower::temporal::static_type(expr, dialect);
	matches!(ty, SqlType::Text | SqlType::Varchar | SqlType::Bpchar | SqlType::Name | SqlType::Char)
		&& declared_text(expr).is_none()
}

/// The PostgreSQL type a value of `expr` is declared as, as text: the `SQL TYPE` of a column or the
/// network type the catalog gives it, the target of a cast to a type with a family of its own, or
/// `bpchar` for a cast to a blank-padded character type.
/// A column whose `SQL TYPE` names a domain is declared as the domain's base type, which the
/// catalog types the column by. `None` when neither says.
fn declared_text(expr: &BoundExpr) -> Option<String> {
	if let BoundExpr::Column(col) = expr {
		let declared = col
			.declared_sql
			.as_deref()
			.filter(|declared| {
				declared.dialect()
					== surrealdb_expr::expr::statements::define::SqlTypeDialect::PostgreSql
			})
			.and_then(|declared| {
				let names_a_domain = SqlType::from_name(declared.base()) == SqlType::Unknown
					&& col.declared_type != SqlType::Unknown;
				if names_a_domain {
					col.declared_type.name()
				} else {
					Some(declared.canonical_text())
				}
			});
		// A column the catalog types as a network has no `SQL TYPE` to name it by.
		return declared.or_else(|| {
			if col.declared_type == SqlType::Inet {
				Some("inet".to_owned())
			} else if col.declared_type == SqlType::Cidr {
				Some("cidr".to_owned())
			} else {
				None
			}
		});
	}
	if let BoundExpr::Cast {
		target_type,
		..
	} = expr
		&& super::family_cast::is_family_target(target_type)
	{
		return Some(target_type.clone());
	}
	// A cast to `char(n)`, `character` or `bpchar` yields a blank-padded value, whose cast to text
	// drops the padding whatever the width.
	if let BoundExpr::Cast {
		target_type,
		..
	} = expr
		&& is_blank_padded(target_type)
	{
		return Some("bpchar".to_owned());
	}
	None
}

/// Whether a cast target names PostgreSQL's blank-padded character type: `char(n)`,
/// `character(n)` or `bpchar`, with or without a width.
pub(crate) fn is_blank_padded(target_type: &str) -> bool {
	SqlType::from_name(target_type.split('(').next().unwrap_or_default().trim()) == SqlType::Bpchar
}

/// A call of the builtin `name`.
fn call(name: &str, args: Vec<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal(name.to_owned()),
		args,
	}))
}

fn string_literal(text: &str) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::String(Strand::from(text)))
}

/// The operand of a MySQL cast to `TIME`, which is an interval: a datetime or timestamp is its time
/// of day, and any other value is left for the cast to read. The operand is evaluated once.
fn mysql_time_operand(value: LogicalExpr) -> LogicalExpr {
	let call = |name: &str, arg: LogicalExpr| {
		LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal(name.to_string()),
			args: vec![arg],
		}))
	};
	let time_of_day = LogicalExpr::Prefix {
		op: PrefixOperator::Cast(Kind::Time),
		expr: Box::new(LogicalExpr::Current),
	};
	LogicalExpr::Optional {
		base: Box::new(value),
		then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(
				call("type::is_datetime", LogicalExpr::Current)
					.or(call("type::is_timestamp", LogicalExpr::Current)),
				call("type::interval", time_of_day),
			)],
			otherwise: Some(LogicalExpr::Current),
		}))),
	}
}

/// Lower a `BoundExpr` to a SurrealDB `LogicalExpr`.
pub fn lower_bound_expr(
	expr: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	match expr {
		BoundExpr::Literal(lit) => match lit {
			BoundLiteral::Integer(i) => Ok(LogicalExpr::Literal(LogicalLiteral::Integer(*i))),
			BoundLiteral::Unsigned(u) => Ok(LogicalExpr::Literal(LogicalLiteral::U64(*u))),
			BoundLiteral::Float(f) => Ok(LogicalExpr::Literal(LogicalLiteral::Float(*f))),
			BoundLiteral::Numeric(text) => Ok(LogicalExpr::Literal(match numeric_value(text)? {
				NumericValue::Decimal(decimal) => LogicalLiteral::Decimal(decimal),
				NumericValue::BigDecimal(big) => LogicalLiteral::BigDecimal(Box::new(big)),
			})),
			BoundLiteral::String(s) => {
				Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(s.as_str()))))
			}
			BoundLiteral::Boolean(b) => Ok(LogicalExpr::Literal(LogicalLiteral::Bool(*b))),
			BoundLiteral::Null => Ok(LogicalExpr::Literal(LogicalLiteral::Null)),
		},
		BoundExpr::Column(col) => {
			if col.id_table.is_some() {
				Ok(lower_id_key(col))
			} else if col.link_table.is_some() {
				Ok(key_of(lower_column_raw(col), true))
			} else {
				Ok(lower_column_raw(col))
			}
		}
		BoundExpr::Access {
			base,
			field,
		} => {
			let base_expr = lower_bound_expr(base, policy)?;
			Ok(LogicalExpr::Access {
				base: Box::new(base_expr),
				field: Strand::from(field.as_str()),
			})
		}
		BoundExpr::Param {
			name,
			..
		} => Ok(LogicalExpr::Param(Param::from(name.clone()))),
		BoundExpr::UserVar(name) => Ok(LogicalExpr::Param(Param::from(format!("user_var_{name}")))),
		BoundExpr::SystemVar {
			name,
			is_global,
		} => lower_system_var(name, *is_global, policy),
		BoundExpr::Binary {
			left,
			op,
			right,
		} => {
			if matches!(op, BoundBinaryOp::Regexp | BoundBinaryOp::RegexpCi) {
				lower_regex_match(left, right, *op == BoundBinaryOp::RegexpCi, false, policy)
			} else if let Some((left, right)) = mysql_widened_operands(left, *op, right, policy) {
				lower_binary_op(&left, *op, &right, policy)
			} else {
				lower_binary_op(left, *op, right, policy)
			}
		}
		BoundExpr::Unary {
			op,
			expr,
		} => {
			if *op == BoundUnaryOp::Not
				&& let BoundExpr::Binary {
					left,
					op: regex_op @ (BoundBinaryOp::Regexp | BoundBinaryOp::RegexpCi),
					right,
				} = &**expr
			{
				return lower_regex_match(
					left,
					right,
					*regex_op == BoundBinaryOp::RegexpCi,
					true,
					policy,
				);
			}
			let inner = lower_bound_expr(expr, policy)?;
			match op {
				BoundUnaryOp::Not => Ok(LogicalExpr::If(Box::new(LogicalIf {
					branches: vec![(
						is_null_expr(inner.clone()),
						LogicalExpr::Literal(LogicalLiteral::Null),
					)],
					otherwise: Some(LogicalExpr::Prefix {
						op: PrefixOperator::Not,
						expr: Box::new(inner),
					}),
				}))),
				BoundUnaryOp::Positive => Ok(inner),
				BoundUnaryOp::Negate => Ok(LogicalExpr::If(Box::new(LogicalIf {
					branches: vec![(
						is_null_expr(inner.clone()),
						LogicalExpr::Literal(LogicalLiteral::Null),
					)],
					otherwise: Some(LogicalExpr::Prefix {
						op: PrefixOperator::Negate,
						expr: Box::new(inner),
					}),
				}))),
			}
		}
		BoundExpr::FunctionCall {
			name,
			args,
			distinct,
		} => {
			if *distinct {
				match name.as_str() {
					"count" => {
						let lowered_arg = if args.len() == 1 {
							lower_bound_expr(&args[0], policy)?
						} else {
							return Err(PlanError::UnsupportedExpression(
								"COUNT(DISTINCT) requires 1 argument".into(),
							));
						};
						// COUNT(DISTINCT x) counts the distinct values of x other than NULL, as
						// COUNT(x) counts the rows whose x is not NULL. A missing x reads as NONE,
						// which array::group keeps apart from NULL, so both are removed.
						let group_call = LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("array::group".to_string()),
							args: vec![lowered_arg],
						}));
						let non_null = LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("array::complement".to_string()),
							args: vec![
								group_call,
								LogicalExpr::Array(vec![
									LogicalExpr::Literal(LogicalLiteral::Null),
									LogicalExpr::Literal(LogicalLiteral::None),
								]),
							],
						}));
						return Ok(LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("array::len".to_string()),
							args: vec![non_null],
						})));
					}
					"sum" => {
						let lowered_arg = if args.len() == 1 {
							lower_bound_expr(&args[0], policy)?
						} else {
							return Err(PlanError::UnsupportedExpression(
								"SUM(DISTINCT) requires 1 argument".into(),
							));
						};
						// The engine's aggregate that folds each distinct number of the group
						// once, and answers NULL for a group with none.
						return Ok(LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("math::sum_distinct".to_string()),
							args: vec![lowered_arg],
						})));
					}
					"avg" => {
						let lowered_arg = if args.len() == 1 {
							lower_bound_expr(&args[0], policy)?
						} else {
							return Err(PlanError::UnsupportedExpression(
								"AVG(DISTINCT) requires 1 argument".into(),
							));
						};
						// The engine's aggregate that folds each distinct number of the group
						// once, and answers NULL for a group with none.
						return Ok(LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("math::mean_distinct".to_string()),
							args: vec![lowered_arg],
						})));
					}
					"min" | "max" => {
						// MIN(DISTINCT x) is identical to MIN(x)
						// MAX(DISTINCT x) is identical to MAX(x)
					}
					"string_agg" | "group_concat" | "array_agg" | "json_agg" | "jsonb_agg"
					| "stddev" | "stddev_samp" | "variance" | "var_samp" | "std" | "stddev_pop"
					| "var_pop" | "json_arrayagg" => {
						return Err(PlanError::UnsupportedExpression(format!(
							"{}(DISTINCT ...)",
							name.to_uppercase()
						)));
					}
					_ => {}
				}
			}

			let func_def = FunctionRegistry::lookup(name, policy.dialect)
				.ok_or_else(|| PlanError::UndefinedFunction(name.clone()))?;

			if let Some(metric) = crate::vector::VectorMetric::from_function(name)
				&& let [a, b] = args.as_slice()
			{
				return Ok(super::vector_expr::lower_distance(
					metric,
					lower_bound_expr(a, policy)?,
					lower_bound_expr(b, policy)?,
				));
			}
			if name == "inner_product"
				&& let [a, b] = args.as_slice()
			{
				return Ok(super::vector_expr::lower_inner_product(
					lower_bound_expr(a, policy)?,
					lower_bound_expr(b, policy)?,
				));
			}
			if let Some(lowered) = crate::lower::functions::lower_function(name, args, policy)? {
				return Ok(lowered);
			}

			// Special built-in lowering logic
			match name.as_str() {
				"coalesce" | "ifnull" => {
					let mut iter = args.iter().rev();
					let Some(last) = iter.next() else {
						return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
					};
					let mut current = lower_bound_expr(last, policy)?;
					for prev in iter {
						let prev_l = lower_bound_expr(prev, policy)?;
						current = LogicalExpr::Binary {
							left: Box::new(prev_l),
							op: BinaryOperator::NullCoalescing,
							right: Box::new(current),
						};
					}
					return Ok(current);
				}
				"nullif" if args.len() == 2 => {
					let l_a = lower_bound_expr(&args[0], policy)?;
					let l_b = lower_bound_expr(&args[1], policy)?;
					return Ok(LogicalExpr::If(Box::new(LogicalIf {
						branches: vec![(
							LogicalExpr::Binary {
								left: Box::new(l_a.clone()),
								op: BinaryOperator::Equal,
								right: Box::new(l_b),
							},
							LogicalExpr::Literal(LogicalLiteral::Null),
						)],
						otherwise: Some(l_a),
					})));
				}
				"unnest" => {
					return Err(PlanError::UnsupportedExpression(
						"UNNEST outside a select-list target".into(),
					));
				}
				crate::functions::BIT_AND => {
					let [value, BoundExpr::Literal(BoundLiteral::Integer(mask))] = args.as_slice()
					else {
						return Err(PlanError::UnsupportedExpression(
							"`&` against anything but an integer constant".into(),
						));
					};
					if *mask <= 0 || mask & (mask - 1) != 0 {
						return Err(PlanError::UnsupportedExpression(
							"`&` with a mask that is not a single bit".into(),
						));
					}
					let value = lower_bound_expr(value, policy)?;
					let integer = |n: i64| LogicalExpr::Literal(LogicalLiteral::Integer(n));
					// The bit is `floor(value / mask) % 2`, scaled back to the mask. The remainder
					// takes the sign of the dividend, so a negative value (two's complement in
					// PostgreSQL) is brought to 0 or 1 by adding the divisor and taking it again.
					let remainder = LogicalExpr::Binary {
						left: Box::new(LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("math::floor".to_string()),
							args: vec![LogicalExpr::Binary {
								left: Box::new(value.clone()),
								op: BinaryOperator::Divide,
								right: Box::new(integer(*mask)),
							}],
						}))),
						op: BinaryOperator::Remainder,
						right: Box::new(integer(2)),
					};
					let bit = LogicalExpr::Binary {
						left: Box::new(LogicalExpr::Binary {
							left: Box::new(remainder),
							op: BinaryOperator::Add,
							right: Box::new(integer(2)),
						}),
						op: BinaryOperator::Remainder,
						right: Box::new(integer(2)),
					};
					return Ok(LogicalExpr::If(Box::new(LogicalIf {
						branches: vec![(
							LogicalExpr::Call(Box::new(LogicalCall {
								receiver: Function::Normal("type::is_number".to_string()),
								args: vec![value],
							})),
							LogicalExpr::Binary {
								left: Box::new(bit),
								op: BinaryOperator::Multiply,
								right: Box::new(integer(*mask)),
							},
						)],
						otherwise: Some(LogicalExpr::Literal(LogicalLiteral::Null)),
					})));
				}
				crate::functions::SUBSCRIPT => {
					let [array, index] = args.as_slice() else {
						return Err(PlanError::UnsupportedExpression("an array subscript".into()));
					};
					// Subscripts count from one, except in a vector column of the catalog, which
					// counts from zero; one outside the array is NULL.
					let first = i64::from(!matches!(
						array,
						BoundExpr::Column(BoundColumnRef {
							declared_type: crate::SqlType::Int2Vector | crate::SqlType::OidVector,
							..
						})
					));
					let index = lower_bound_expr(index, policy)?;
					let null = || LogicalExpr::Literal(LogicalLiteral::Null);
					return Ok(LogicalExpr::If(Box::new(LogicalIf {
						branches: vec![(
							LogicalExpr::Binary {
								left: Box::new(index.clone()),
								op: BinaryOperator::MoreThanEqual,
								right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(
									first,
								))),
							},
							LogicalExpr::Binary {
								left: Box::new(LogicalExpr::Index {
									base: Box::new(lower_bound_expr(array, policy)?),
									index: Box::new(LogicalExpr::Binary {
										left: Box::new(index),
										op: BinaryOperator::Subtract,
										right: Box::new(LogicalExpr::Literal(
											LogicalLiteral::Integer(first),
										)),
									}),
								}),
								op: BinaryOperator::NullCoalescing,
								right: Box::new(null()),
							},
						)],
						otherwise: Some(null()),
					})));
				}
				"generate_subscripts" => {
					return Err(PlanError::UnsupportedExpression(
						"GENERATE_SUBSCRIPTS other than as a bare select-list target".into(),
					));
				}
				crate::functions::QUANTIFIED_ARRAY => {
					let [
						value,
						array,
						BoundExpr::Literal(BoundLiteral::Boolean(equal)),
						BoundExpr::Literal(BoundLiteral::Boolean(all)),
					] = args.as_slice()
					else {
						return Err(PlanError::UnsupportedExpression(
							"a quantified comparison against an array".into(),
						));
					};
					return Ok(sql_quantified(
						lower_bound_expr(value, policy)?,
						lower_bound_expr(array, policy)?,
						*equal,
						*all,
					));
				}
				"if" if args.len() == 3 => {
					let cond = lower_bound_expr(&args[0], policy)?;
					let then_val = lower_bound_expr(&args[1], policy)?;
					let else_val = lower_bound_expr(&args[2], policy)?;
					return Ok(LogicalExpr::If(Box::new(LogicalIf {
						branches: vec![(cond, then_val)],
						otherwise: Some(else_val),
					})));
				}
				"greatest" if !args.is_empty() => {
					let lowered_args = args
						.iter()
						.map(|a| lower_bound_expr(a, policy))
						.collect::<Result<Vec<_>, _>>()?;
					return Ok(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("math::max".to_string()),
						args: vec![LogicalExpr::Array(lowered_args)],
					})));
				}
				"least" if !args.is_empty() => {
					let lowered_args = args
						.iter()
						.map(|a| lower_bound_expr(a, policy))
						.collect::<Result<Vec<_>, _>>()?;
					return Ok(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("math::min".to_string()),
						args: vec![LogicalExpr::Array(lowered_args)],
					})));
				}
				"version" => {
					let ver = match policy.dialect {
						SqlDialect::MySql => MYSQL_SERVER_VERSION,
						SqlDialect::Postgres => "PostgreSQL 16.0 (SurrealDB)",
					};
					return Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(ver))));
				}
				"connection_id" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Integer(1)));
				}
				"last_insert_id" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Integer(0)));
				}
				"row_count" | "found_rows" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Integer(0)));
				}
				"curdate" | "curtime" | "utc_timestamp" => {
					return Ok(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("time::now".to_string()),
						args: vec![],
					})));
				}
				"unix_timestamp" => {
					if args.is_empty() {
						let now_call = LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("time::now".to_string()),
							args: vec![],
						}));
						return Ok(LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("time::unix".to_string()),
							args: vec![now_call],
						})));
					} else {
						let arg = lower_bound_expr(&args[0], policy)?;
						return Ok(LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("time::unix".to_string()),
							args: vec![arg],
						})));
					}
				}
				"from_unixtime" if !args.is_empty() => {
					let arg = lower_bound_expr(&args[0], policy)?;
					return Ok(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("time::from::unix".to_string()),
						args: vec![arg],
					})));
				}
				"regexp" | "regexp_like" if args.len() >= 2 => {
					// MySQL matches without regard to case unless a third argument says `c`.
					let case_insensitive = match args.get(2) {
						None => true,
						// A NULL argument makes the whole match NULL.
						Some(BoundExpr::Literal(BoundLiteral::Null)) => {
							return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
						}
						Some(BoundExpr::Literal(BoundLiteral::String(flags))) => {
							let mut ignore_case = true;
							for flag in flags.chars() {
								match flag {
									'c' => ignore_case = false,
									'i' => ignore_case = true,
									_ => {
										return Err(PlanError::UnsupportedExpression(format!(
											"match type '{flag}' of {name}"
										)));
									}
								}
							}
							ignore_case
						}
						Some(_) => {
							return Err(PlanError::UnsupportedExpression(format!(
								"a match type of {name} that is not a string literal"
							)));
						}
					};
					return lower_regex_match(&args[0], &args[1], case_insensitive, false, policy);
				}
				"current_database" | "database" | "schema" => {
					return Ok(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("session::db".to_string()),
						args: Vec::new(),
					})));
				}
				"current_schema" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(
						"public",
					))));
				}
				"current_schemas" => {
					let include_implicit = match args.first() {
						Some(BoundExpr::Literal(BoundLiteral::Boolean(b))) => *b,
						_ => true,
					};
					let schemas = if include_implicit {
						vec![
							LogicalExpr::Literal(LogicalLiteral::String(Strand::from(
								"pg_catalog",
							))),
							LogicalExpr::Literal(LogicalLiteral::String(Strand::from("public"))),
						]
					} else {
						vec![LogicalExpr::Literal(LogicalLiteral::String(Strand::from("public")))]
					};
					return Ok(LogicalExpr::Array(schemas));
				}
				"current_user" | "session_user" | "user" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from("root"))));
				}
				"pg_backend_pid" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Integer(
						std::process::id() as i64
					)));
				}
				"pg_is_in_recovery" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Bool(false)));
				}
				"to_regtype" if args.len() == 1 => {
					// The OID of the type a name spells, or NULL for a name that is none the
					// catalog lists; a name that is known only when the statement runs is looked
					// up then.
					let name = lower_bound_expr(&args[0], policy)?;
					let mut lookup = LogicalExpr::Literal(LogicalLiteral::Null);
					for (type_name, oid) in crate::catalog::system::REGTYPE_NAMES.iter().rev() {
						lookup = LogicalExpr::If(Box::new(LogicalIf {
							branches: vec![(
								LogicalExpr::Binary {
									left: Box::new(LogicalExpr::Current),
									op: BinaryOperator::Equal,
									right: Box::new(LogicalExpr::Literal(LogicalLiteral::String(
										Strand::from(*type_name),
									))),
								},
								LogicalExpr::Literal(LogicalLiteral::Integer(i64::from(*oid))),
							)],
							otherwise: Some(lookup),
						}));
					}
					return Ok(LogicalExpr::Optional {
						base: Box::new(name),
						then: Box::new(lookup),
					});
				}
				"pg_get_userbyid" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from("root"))));
				}
				"pg_get_statisticsobjdef_columns" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
				}
				"pg_relation_is_publishable" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Bool(false)));
				}
				"pg_partition_ancestors" if args.len() == 1 => {
					// No table is partitioned, so a relation's ancestors are the relation alone.
					if let BoundExpr::Literal(BoundLiteral::String(text)) = &args[0]
						&& let Ok(oid) = text.trim().parse::<i64>()
					{
						return Ok(LogicalExpr::Literal(LogicalLiteral::Integer(oid)));
					}
					return lower_bound_expr(&args[0], policy);
				}
				"pg_encoding_to_char" if args.len() == 1 => {
					// The server encoding is UTF8, whose number is 6; an unknown number has no
					// name.
					let encoding = lower_bound_expr(&args[0], policy)?;
					let utf8 = LogicalExpr::Binary {
						left: Box::new(encoding),
						op: BinaryOperator::Equal,
						right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(6))),
					};
					return Ok(LogicalExpr::If(Box::new(LogicalIf {
						branches: vec![(
							utf8,
							LogicalExpr::Literal(LogicalLiteral::String(Strand::from("UTF8"))),
						)],
						otherwise: Some(LogicalExpr::Literal(LogicalLiteral::String(
							Strand::from(""),
						))),
					})));
				}
				"format_type" if !args.is_empty() => {
					if let BoundExpr::Literal(BoundLiteral::Integer(oid)) = &args[0] {
						let name = PG_TYPES
							.iter()
							.find(|(type_oid, ..)| type_oid == oid)
							.map(|(_, _, name)| (*name).to_owned())
							.or_else(|| crate::reported_type::pg_format_type(*oid, -1))
							.unwrap_or_else(|| "text".to_owned());
						return Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(
							name.as_str(),
						))));
					}
					// A NULL or unknown oid matches no branch and yields NULL.
					let oid = lower_bound_expr(&args[0], policy)?;
					let known = PG_TYPES.iter().map(|(type_oid, _, name)| (*type_oid, *name));
					let others = other_builtin_type_names()
						.iter()
						.filter(|(type_oid, _)| {
							PG_TYPES.iter().all(|(listed, ..)| listed != type_oid)
						})
						.map(|(type_oid, name)| (*type_oid, name.as_str()));
					let branches = known
						.chain(others)
						.map(|(type_oid, name)| {
							(
								LogicalExpr::Binary {
									left: Box::new(oid.clone()),
									op: BinaryOperator::Equal,
									right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(
										type_oid,
									))),
								},
								LogicalExpr::Literal(LogicalLiteral::String(Strand::from(name))),
							)
						})
						.collect();
					return Ok(LogicalExpr::If(Box::new(LogicalIf {
						branches,
						otherwise: None,
					})));
				}
				"pg_table_is_visible"
				| "pg_type_is_visible"
				| "pg_function_is_visible"
				| "has_schema_privilege"
				| "has_table_privilege"
				| "has_database_privilege"
				| "has_column_privilege" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Bool(true)));
				}
				"quote_ident" if args.len() == 1 => {
					return Ok(match &args[0] {
						BoundExpr::Literal(BoundLiteral::String(s)) => LogicalExpr::Literal(
							LogicalLiteral::String(Strand::from(quote_ident_str(s).as_str())),
						),
						BoundExpr::Literal(BoundLiteral::Null) => {
							LogicalExpr::Literal(LogicalLiteral::Null)
						}
						arg @ BoundExpr::Literal(_)
						| arg @ BoundExpr::Column(_)
						| arg @ BoundExpr::Access {
							..
						}
						| arg @ BoundExpr::Param {
							..
						}
						| arg @ BoundExpr::Binary {
							..
						}
						| arg @ BoundExpr::Unary {
							..
						}
						| arg @ BoundExpr::FunctionCall {
							..
						}
						| arg @ BoundExpr::Case {
							..
						}
						| arg @ BoundExpr::Between {
							..
						}
						| arg @ BoundExpr::InList {
							..
						}
						| arg @ BoundExpr::IsNull {
							..
						}
						| arg @ BoundExpr::Cast {
							..
						}
						| arg @ BoundExpr::ScalarSubquery(_)
						| arg @ BoundExpr::Exists(_)
						| arg @ BoundExpr::InSubquery {
							..
						}
						| arg @ BoundExpr::ArraySubquery(_)
						| arg @ BoundExpr::WindowFunction {
							..
						}
						| arg @ BoundExpr::UserVar(_)
						| arg @ BoundExpr::SystemVar {
							..
						} => lower_quote_ident(lower_bound_expr(arg, policy)?),
					});
				}
				"obj_description" | "col_description" | "shobj_description" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
				}
				// The catalog stores an expression as the text `pg_get_expr` answers, and lists no
				// expression it does not store that way (`indexprs` and `indpred` are NULL).
				"pg_get_expr" if !args.is_empty() => {
					return lower_bound_expr(&args[0], policy);
				}
				// A lock waits on other sessions, which a query cannot; the wire front end runs
				// a statement that is one call and passes nothing else here.
				"pg_advisory_lock"
				| "pg_try_advisory_lock"
				| "pg_advisory_unlock"
				| "pg_advisory_unlock_all" => {
					return Err(PlanError::UnsupportedExpression(format!(
						"{name} is supported only as a whole statement, `SELECT {name}(...)`"
					)));
				}
				// A function listed in `pg_proc` is built in and has no source to render.
				"pg_get_functiondef" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
				}
				// Every index is over plain columns, so no index column is an expression.
				"pg_get_indexdef" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
				}
				"pg_relation_size" | "pg_total_relation_size" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Integer(0)));
				}
				// A planned statement has no session to keep the setting in: the call answers
				// with the value it was asked to set, as the function does.
				"set_config" => {
					return lower_bound_expr(&args[1], policy);
				}
				"current_setting" => {
					let BoundExpr::Literal(BoundLiteral::String(s)) = &args[0] else {
						return Err(PlanError::UnsupportedExpression(
							"current_setting requires a string literal argument".into(),
						));
					};
					let name_str = s.as_str();
					let val = match name_str.to_ascii_lowercase().as_str() {
						"timezone" | "time zone" => "UTC",
						"server_version" => "16.0-surrealdb",
						"server_version_num" => "160000",
						"server_encoding" => "UTF8",
						"client_encoding" => "UTF8",
						"datestyle" => "ISO, MDY",
						"integer_datetimes" => "on",
						"standard_conforming_strings" => "on",
						"application_name" => "",
						"search_path" => "\"$user\", public",
						"extra_float_digits" => "1",
						"bytea_output" => "hex",
						"intervalstyle" => "postgres",
						"is_superuser" => "on",
						"session_authorization" => "root",
						_ => {
							if args.len() > 1 {
								return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
							} else {
								return Err(PlanError::UndefinedObject(format!(
									"unrecognized configuration parameter \"{name_str}\""
								)));
							}
						}
					};
					return Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(val))));
				}
				"jsonb_build_object" | "json_build_object" | "json_object" => {
					let mut obj = Vec::new();
					for chunk in args.chunks(2) {
						let key = if let BoundExpr::Literal(BoundLiteral::String(s)) = &chunk[0] {
							s.clone()
						} else {
							let k_low = lower_bound_expr(&chunk[0], policy)?;
							format!("{k_low}")
						};
						let val = if chunk.len() > 1 {
							lower_bound_expr(&chunk[1], policy)?
						} else {
							LogicalExpr::Literal(LogicalLiteral::Null)
						};
						obj.push((key, val));
					}
					return Ok(LogicalExpr::Object(obj));
				}
				"json_array" => {
					let items = args
						.iter()
						.map(|a| lower_bound_expr(a, policy))
						.collect::<Result<Vec<_>, _>>()?;
					return Ok(LogicalExpr::Array(items));
				}
				_ => {}
			}

			let mapped_fn = func_def.internal_name.unwrap_or(func_def.name);
			if matches!(func_def.name, "min" | "max")
				&& let [arg] = args.as_slice()
				&& let Some(reference) = scalar_enum(arg)
			{
				return Ok(extreme(
					reference,
					lower_bound_expr(arg, policy)?,
					func_def.name == "max",
				));
			}
			if func_def.name == "group_concat" && args.len() > 1 {
				return Err(PlanError::UnsupportedExpression(
					"GROUP_CONCAT over several expressions".into(),
				));
			}
			let lower_args = if mapped_fn == "count"
				&& (args.is_empty()
					|| (args.len() == 1
						&& (matches!(&args[0], BoundExpr::Column(col) if counts_every_row(col))
							|| matches!(&args[0], BoundExpr::Literal(BoundLiteral::Integer(_))))))
			{
				Vec::new()
			} else if mapped_fn == "count" && args.len() == 1 {
				// The engine's aggregate that counts the values that are neither NULL nor NONE,
				// folding the argument as written: a per-row test inside the fold would keep the
				// scan from running on the stored row.
				return Ok(LogicalExpr::Call(Box::new(LogicalCall {
					receiver: Function::Normal("count::present".to_string()),
					args: vec![lower_stored(&args[0], policy)?],
				})));
			} else {
				args.iter().map(|a| lower_bound_expr(a, policy)).collect::<Result<Vec<_>, _>>()?
			};
			// A string function fails on a NULL argument where SQL answers NULL. An argument that
			// is a column, a literal or a parameter is read again in the test rather than bound,
			// so that the call stays a plain call over the stored row, as an index key needs.
			let string_function = func_def.is_scalar()
				&& matches!(
					mapped_fn,
					"string::lowercase"
						| "crypto::md5" | "crypto::sha1"
						| "string::uppercase"
						| "string::len" | "string::reverse"
						| "string::replace"
						| "string::repeat"
				);
			let present_literals = args.iter().all(
				|arg| matches!(arg, BoundExpr::Literal(literal) if !matches!(literal, BoundLiteral::Null)),
			);
			let plain_arguments = args.iter().all(|arg| {
				matches!(
					arg,
					BoundExpr::Column(_) | BoundExpr::Literal(_) | BoundExpr::Param { .. }
				)
			});
			if string_function && !lower_args.is_empty() && !present_literals && plain_arguments {
				let null = lower_args
					.iter()
					.cloned()
					.map(is_null_expr)
					.reduce(LogicalExpr::or)
					.unwrap_or(LogicalExpr::Literal(LogicalLiteral::Bool(false)));
				return Ok(LogicalExpr::If(Box::new(LogicalIf {
					branches: vec![(null, LogicalExpr::Literal(LogicalLiteral::Null))],
					otherwise: Some(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal(mapped_fn.to_string()),
						args: lower_args,
					}))),
				})));
			}
			if func_def.is_scalar()
				&& (mapped_fn.starts_with("math::") || (string_function && !present_literals))
				&& !lower_args.is_empty()
				&& !args.iter().all(|arg| {
					matches!(
						arg,
						BoundExpr::Literal(BoundLiteral::Integer(_) | BoundLiteral::Float(_))
					)
				}) {
				let refs: Vec<_> =
					(0..lower_args.len()).map(|index| current_item(index as i64)).collect();
				let null = refs
					.iter()
					.cloned()
					.map(is_null_expr)
					.reduce(LogicalExpr::or)
					.unwrap_or(LogicalExpr::Literal(LogicalLiteral::Bool(false)));
				return Ok(LogicalExpr::Optional {
					base: Box::new(LogicalExpr::Array(lower_args)),
					then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
						branches: vec![(null, LogicalExpr::Literal(LogicalLiteral::Null))],
						otherwise: Some(LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal(mapped_fn.to_string()),
							args: refs,
						}))),
					}))),
				});
			}
			// The value that decides whether the group holds any, as written.
			let counted = (matches!(mapped_fn, "math::sum" | "math::mean" | "array::join")
				&& !lower_args.is_empty())
			.then(|| lower_args[0].clone());
			let lower_args = if mapped_fn == "array::join" && !lower_args.is_empty() {
				// The value joins as text, so that a float or a decimal carries no SurrealQL
				// suffix and NULL stays NULL, which the join skips. A NULL separator is none, and
				// `GROUP_CONCAT(x)` joins on a comma.
				let mut given = lower_args.into_iter();
				let value = given.next().into_iter().map(|value| LogicalExpr::Optional {
					base: Box::new(value),
					then: Box::new(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("type::string".to_string()),
						args: vec![LogicalExpr::Current],
					}))),
				});
				let separator = match given.next() {
					Some(separator) => LogicalExpr::Binary {
						left: Box::new(separator),
						op: BinaryOperator::NullCoalescing,
						right: Box::new(LogicalExpr::Literal(LogicalLiteral::String(
							Strand::from(""),
						))),
					},
					None => LogicalExpr::Literal(LogicalLiteral::String(Strand::from(","))),
				};
				value.chain(std::iter::once(separator)).collect()
			} else {
				lower_args
			};
			let call = LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal(mapped_fn.to_string()),
				args: lower_args,
			}));
			// The engine folds no value to 0 (and 0/0 to NaN), joins none to the empty string and
			// no value to NONE for MIN and MAX; SQL answers NULL when the group holds no value at
			// all, and an array aggregate answers NULL over no row. `array::min` skips NULL and
			// NONE and answers NONE where nothing is left, so it witnesses a group with no value
			// while folding the argument as written: a per-row test inside the fold would keep
			// the scan from running on the stored row.
			let empty = match counted {
				Some(value) => Some(LogicalExpr::Binary {
					left: Box::new(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("array::min".to_string()),
						args: vec![value],
					}))),
					op: BinaryOperator::Equal,
					right: Box::new(LogicalExpr::Literal(LogicalLiteral::None)),
				}),
				None if mapped_fn == "array::collect" => Some(LogicalExpr::Binary {
					left: Box::new(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("count".to_string()),
						args: Vec::new(),
					}))),
					op: BinaryOperator::Equal,
					right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(0))),
				}),
				None => None,
			};
			if let Some(empty) = empty {
				return Ok(LogicalExpr::If(Box::new(LogicalIf {
					branches: vec![(empty, LogicalExpr::Literal(LogicalLiteral::Null))],
					otherwise: Some(call),
				})));
			}
			if matches!(mapped_fn, "array::min" | "array::max") {
				return Ok(LogicalExpr::Binary {
					left: Box::new(call),
					op: BinaryOperator::NullCoalescing,
					right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
				});
			}
			Ok(call)
		}
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => {
			let mut lower_branches = Vec::new();
			for (cond, val) in branches {
				let condition = if let Some(base) = operand {
					let subject = lower_bound_expr(base, policy)?;
					let mut candidate = lower_bound_expr(cond, policy)?;
					if let Some(truth) = pg_bool_spelling(cond) {
						candidate = bool_if_boolean(&subject, truth, candidate);
					}
					LogicalExpr::Binary {
						left: Box::new(subject),
						op: BinaryOperator::Equal,
						right: Box::new(candidate),
					}
				} else {
					lower_bound_expr(cond, policy)?
				};
				let value = lower_bound_expr(val, policy)?;
				lower_branches.push((condition, value));
			}
			let otherwise = else_expr.as_ref().map(|e| lower_bound_expr(e, policy)).transpose()?;
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: lower_branches,
				otherwise,
			})))
		}

		BoundExpr::Between {
			expr,
			low,
			high,
			negated,
		} => {
			let promoted =
				super::temporal::promoted_group(&[&**expr, &**low, &**high], policy.dialect);
			let (value, low_bound, high_bound) = (
				lower_bound_expr(super::temporal::operand_at(&promoted, 0, expr), policy)?,
				lower_bound_expr(super::temporal::operand_at(&promoted, 1, low), policy)?,
				lower_bound_expr(super::temporal::operand_at(&promoted, 2, high), policy)?,
			);
			let reference =
				scalar_enum(expr).or_else(|| scalar_enum(low)).or_else(|| scalar_enum(high));
			let network = common_network([&**expr, &**low, &**high].into_iter());
			Ok(match (reference, network) {
				(Some(reference), _) => sql_between(
					rank(reference, value),
					rank(reference, low_bound),
					rank(reference, high_bound),
					*negated,
				),
				(None, Some(network)) => {
					refuse_non_network_peers(network, [&**expr, &**low, &**high], ">=")?;
					sql_between(
						network_key(network_operand(network, expr, value)?),
						network_key(network_operand(network, low, low_bound)?),
						network_key(network_operand(network, high, high_bound)?),
						*negated,
					)
				}
				(None, None) => sql_between(value, low_bound, high_bound, *negated),
			})
		}
		BoundExpr::InList {
			expr,
			list,
			negated,
		} => {
			if let Some((col, table)) = id_column(expr)
				&& let Some((records, true)) =
					id_member_records(table, col.key_read, list, policy, true)?
			{
				return Ok(sql_membership(
					lower_column_raw(col),
					LogicalExpr::Array(records),
					*negated,
				));
			}
			if let Some((col, table)) = link_column(expr) {
				return lower_link_membership(col, table, list, *negated, policy);
			}
			let operands: Vec<&BoundExpr> = std::iter::once(&**expr).chain(list.iter()).collect();
			let promoted = super::temporal::promoted_group(&operands, policy.dialect);
			let operands = promoted
				.as_ref()
				.map_or_else(|| operands.clone(), |promoted| promoted.iter().collect());
			let val = lower_bound_expr(operands[0], policy)?;
			let items = operands[1..]
				.iter()
				.map(|item| lower_bound_expr(item, policy))
				.collect::<Result<Vec<_>, _>>()?;
			// The subject and the items take one network type when any of them is a network, as
			// PostgreSQL resolves an untyped subject from the items.
			if let Some(network) = common_network(std::iter::once(&**expr).chain(list)) {
				refuse_non_network_peers(network, std::iter::once(&**expr).chain(list), "=")?;
				return lower_network_membership(network, expr, val, list, items, *negated);
			}
			Ok(sql_membership(val, LogicalExpr::Array(items), *negated))
		}
		BoundExpr::IsNull {
			expr,
			negated,
		} => {
			let is_null = lower_null_test(expr, policy)?;
			if *negated {
				Ok(LogicalExpr::Prefix {
					op: PrefixOperator::Not,
					expr: Box::new(is_null),
				})
			} else {
				Ok(is_null)
			}
		}
		BoundExpr::Cast {
			expr,
			target_type,
		} => {
			if policy.dialect == SqlDialect::Postgres {
				// An interval cast with a field qualifier or a precision is the cast to `interval`,
				// truncated and rounded to the declared type.
				if let Some(declared) = super::functions::interval_typmod(target_type) {
					let plain = BoundExpr::Cast {
						expr: expr.clone(),
						target_type: "interval".to_owned(),
					};
					let lowered = lower_bound_expr(&plain, policy)?;
					return Ok(super::functions::adjust_interval(lowered, &declared));
				}
				if let BoundExpr::Literal(BoundLiteral::String(text)) = expr.as_ref()
					&& let Some(lowered) = super::family_cast::lower_text_cast(text, target_type)
				{
					return lowered;
				}
				if let Some(lowered) = super::family_cast::lower_cast_to_text(expr, target_type) {
					return lowered;
				}
			}
			let lowered = lower_bound_expr(expr, policy)?;
			// The text form of an array of enum labels is read when the expression is lowered.
			let lowered = if let BoundExpr::Literal(BoundLiteral::String(text)) = expr.as_ref()
				&& target_type.starts_with("enum:")
				&& target_type.ends_with("[]")
			{
				let items = parse_text_array_literal(text).ok_or_else(|| {
					PlanError::InvalidTextRepresentation(format!(
						"malformed array literal: \"{text}\""
					))
				})?;
				LogicalExpr::Array(
					items
						.into_iter()
						.map(|item| match item {
							Some(label) => LogicalExpr::Literal(LogicalLiteral::String(
								Strand::from(label.as_str()),
							)),
							None => LogicalExpr::Literal(LogicalLiteral::Null),
						})
						.collect(),
				)
			} else {
				lowered
			};
			let clean_type = target_type.to_lowercase();
			if clean_type == "regtype" {
				return Ok(lower_regtype(lowered));
			}
			if clean_type == "regtype[]"
				&& let BoundExpr::Literal(BoundLiteral::String(text)) = expr.as_ref()
			{
				return lower_regtype_array_literal(text);
			}
			if let Some(array) = lower_multidim_array_cast(expr, target_type)? {
				return Ok(array);
			}
			let base_type = clean_type.split('(').next().unwrap_or(&clean_type).trim();
			if base_type == "vector" {
				let dimensions = clean_type
					.split_once('(')
					.and_then(|(_, rest)| rest.strip_suffix(')'))
					.and_then(|count| count.trim().parse::<usize>().ok());
				return super::vector_expr::lower_cast(lowered, dimensions);
			}
			// A value read at run time is cast to a type with a family of its own as a write to a
			// column of the type reads it: its text is parsed and the value canonicalised.
			if policy.dialect == SqlDialect::Postgres
				&& super::family_cast::is_family_target(&clean_type)
			{
				return Ok(call(
					"sql::fit",
					vec![lowered, string_literal(&clean_type), string_literal("postgresql")],
				));
			}
			let kind = cast_kind(target_type, policy.dialect);
			if matches!(kind, Kind::Any) {
				return Ok(lowered);
			}
			// A cast to text writes the value as PostgreSQL writes it, by its declared type when
			// the operand has one: a date as `2026-10-27`, a numeric with its scale, a range in
			// brackets, an `inet` with its prefix.
			if policy.dialect == SqlDialect::Postgres
				&& matches!(kind, Kind::String)
				&& !is_text_operand(expr, policy.dialect)
			{
				return Ok(call(
					"sql::text",
					vec![lowered, string_literal(&declared_text(expr).unwrap_or_default())],
				));
			}
			if matches!(lowered, LogicalExpr::Literal(LogicalLiteral::Null | LogicalLiteral::None))
			{
				return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
			}
			// An operand that is already a date, time, timestamp or interval needs no reading of
			// its text.
			let operand_is_temporal =
				crate::lower::temporal::static_type(expr, policy.dialect).is_temporal();
			let lowered = if matches!(kind, Kind::Datetime)
				|| (matches!(kind, Kind::Date | Kind::Timestamp) && !operand_is_temporal)
			{
				crate::lower::functions::datetime_cast_operand(lowered)
			} else {
				lowered
			};
			let lowered = crate::lower::functions::timetz_cast_operand(&clean_type, lowered);
			let lowered = if matches!(kind, Kind::Time) {
				crate::lower::functions::time_of_timestamp_text(lowered)
			} else {
				lowered
			};
			let lowered = if policy.dialect == SqlDialect::MySql
				&& matches!(kind, Kind::Interval)
				&& !matches!(lowered, LogicalExpr::Literal(_))
			{
				mysql_time_operand(lowered)
			} else {
				lowered
			};
			// A cast to `numeric(p,s)` rounds to the scale and refuses more integer digits than
			// the precision leaves, as a write to a column of the type does.
			if let Some(scaled) = scaled_numeric(&clean_type, policy.dialect) {
				let cast = if matches!(lowered, LogicalExpr::Literal(_)) {
					LogicalExpr::Prefix {
						op: PrefixOperator::Cast(kind),
						expr: Box::new(lowered),
					}
				} else {
					LogicalExpr::Optional {
						base: Box::new(lowered),
						then: Box::new(LogicalExpr::Prefix {
							op: PrefixOperator::Cast(kind),
							expr: Box::new(LogicalExpr::Current),
						}),
					}
				};
				let dialect = match policy.dialect {
					SqlDialect::Postgres => "postgresql",
					SqlDialect::MySql => "mysql",
				};
				return Ok(call(
					"sql::fit",
					vec![cast, string_literal(&scaled), string_literal(dialect)],
				));
			}
			if matches!(lowered, LogicalExpr::Literal(_)) {
				return Ok(LogicalExpr::Prefix {
					op: PrefixOperator::Cast(kind),
					expr: Box::new(lowered),
				});
			}
			// A cast of NULL or NONE is NULL. The operand is evaluated once and the cast applies to
			// it as `Current`, so chained casts grow linearly; `??` maps the NONE that `Optional`
			// passes through to NULL.
			Ok(LogicalExpr::Binary {
				left: Box::new(LogicalExpr::Optional {
					base: Box::new(lowered),
					then: Box::new(LogicalExpr::Prefix {
						op: PrefixOperator::Cast(kind),
						expr: Box::new(LogicalExpr::Current),
					}),
				}),
				op: BinaryOperator::NullCoalescing,
				right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
			})
		}
		BoundExpr::ScalarSubquery(subquery) => {
			// Two rows are enough to tell more than one from one, so a subquery with no LIMIT of
			// its own yields no more than two, and is copied only to carry that LIMIT. A LIMIT of
			// its own is kept as bound and the check applies to the rows it leaves: `LIMIT 1`
			// never fails and `LIMIT 5` over several rows does.
			let mut sub = std::borrow::Cow::Borrowed(&**subquery);
			if sub.limit.is_none() {
				sub.to_mut().limit = Some(BoundExpr::Literal(BoundLiteral::Integer(2)));
			}
			let rows = LogicalExpr::Subquery(Box::new(lower_bound_subquery(&sub, policy)?));
			Ok(one_row_or_error(rows, policy))
		}
		BoundExpr::Exists(subquery) => {
			let mut sub = *subquery.clone();
			if sub.limit.is_none() {
				sub.limit = Some(BoundExpr::Literal(BoundLiteral::Integer(1)));
			}
			let sub_op = lower_bound_subquery(&sub, policy)?;
			let len_call = LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal("array::len".to_string()),
				args: vec![LogicalExpr::Subquery(Box::new(sub_op))],
			}));
			Ok(LogicalExpr::Binary {
				left: Box::new(len_call),
				op: BinaryOperator::MoreThan,
				right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(0))),
			})
		}
		BoundExpr::InSubquery {
			expr,
			subquery,
			negated,
		} => {
			// The values of a subquery are keys, so a link column is compared by its key.
			let val = match link_column(expr) {
				Some((col, _)) => key_of(lower_column_raw(col), true),
				None => lower_bound_expr(expr, policy)?,
			};
			let sub_op = lower_bound_subquery(subquery, policy)?;
			Ok(sql_membership(val, LogicalExpr::Subquery(Box::new(sub_op)), *negated))
		}
		BoundExpr::ArraySubquery(subquery) => {
			let sub_op = lower_bound_subquery(subquery, policy)?;
			Ok(LogicalExpr::Subquery(Box::new(sub_op)))
		}
		// A select computes its window calls with `Window` operators and reads each from the field
		// it is computed into, so a call reaches here only from a clause that has no rows to
		// compute it over.
		BoundExpr::WindowFunction {
			..
		} => Err(PlanError::WindowNotAllowed("this context".into())),
	}
}

/// The value of a subquery used as a value, from `rows`, the array of its rows: the row when there
/// is one, NONE when there is none, and an error carrying the dialect's message when there is more
/// than one. `rows` is evaluated once, as the value the check reads.
fn one_row_or_error(rows: LogicalExpr, policy: &DialectPolicy) -> LogicalExpr {
	let more_than_one = LogicalExpr::Binary {
		left: Box::new(LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal("array::len".to_string()),
			args: vec![LogicalExpr::Current],
		}))),
		op: BinaryOperator::MoreThan,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(1))),
	};
	let message = match policy.dialect {
		SqlDialect::Postgres => "more than one row returned by a subquery used as an expression",
		SqlDialect::MySql => "Subquery returns more than 1 row",
	};
	LogicalExpr::Optional {
		base: Box::new(rows),
		then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(
				more_than_one,
				LogicalExpr::Throw(Box::new(LogicalExpr::Literal(LogicalLiteral::String(
					Strand::from(message),
				)))),
			)],
			otherwise: Some(LogicalExpr::First(Box::new(LogicalExpr::Current))),
		}))),
	}
}

/// Whether `col`, the argument of a `count`, is a row every row of the select holds: the `*` of
/// `count(*)`, or the whole row of the `FROM` relation, which no join leaves absent. A joined
/// relation's row (`r.*`) is absent where an outer join matched none, and is counted as a value.
pub(crate) fn counts_every_row(col: &BoundColumnRef) -> bool {
	col.column == "*" && col.relation_index == 0 && col.correlation_depth == 0
}

/// Lower a column reference to the stored field it names, with no key extraction.
///
/// A column of the query directly above is read from `$parent`, the row of that query. A column of
/// a query further up is read from the parameter the query below it binds to its value
/// ([`BoundColumnRef::carried_as`]), as `$parent` names no row beyond the one above.
pub(crate) fn lower_column_raw(col: &BoundColumnRef) -> LogicalExpr {
	if let Some(carried) = &col.carried_as {
		LogicalExpr::Param(Param::from(carried.clone()))
	} else if col.correlation_depth > 0 {
		LogicalExpr::Access {
			base: Box::new(LogicalExpr::Param(Param::from("parent".to_string()))),
			field: Strand::from(col.column.as_str()),
		}
	} else if col.column == "*" && col.relation_index > 0 {
		// A joined relation's whole row, which an outer join leaves absent where it matched none.
		RelationAlias::new(col.relation_index - 1).field()
	} else if col.column == "*" {
		LogicalExpr::Field(Strand::from("*"))
	} else {
		// The value of the first relation that holds one: the relations of `also_in` are read
		// only when the schema left it open which relation owns the column.
		col.also_in.iter().fold(
			relation_field(col.relation_index, &col.column),
			|read, relation| LogicalExpr::Binary {
				left: Box::new(read),
				op: BinaryOperator::NullCoalescing,
				right: Box::new(relation_field(*relation, &col.column)),
			},
		)
	}
}

/// Lower a read of `col`, a column of the query directly above, from the row of that query
/// (`$parent`): the stored field of a base table, and for a joined relation the field under the
/// relation's alias, as the row of a join holds it.
pub(crate) fn lower_parent_column(col: &BoundColumnRef) -> LogicalExpr {
	if col.relation_index == 0 {
		return lower_column_raw(col);
	}
	let joined = LogicalExpr::Access {
		base: Box::new(LogicalExpr::Param(Param::from("parent".to_string()))),
		field: RelationAlias::new(col.relation_index - 1).name().clone(),
	};
	if col.column == "*" {
		joined
	} else {
		LogicalExpr::Access {
			base: Box::new(joined),
			field: Strand::from(col.column.as_str()),
		}
	}
}

/// The field `column` of the relation at `relation` of a FROM clause: a base table's fields are on
/// the row itself, a joined table's under its alias.
fn relation_field(relation: usize, column: &str) -> LogicalExpr {
	if relation == 0 {
		LogicalExpr::Field(Strand::from(column))
	} else {
		RelationAlias::new(relation - 1).access(Strand::from(column))
	}
}

/// The records of `table` that the members of an `IN` list of the `id` of `table` name, and whether
/// some member is a column or an expression, not a literal or a parameter. `None` when a member is
/// neither: a literal that is no key, or an expression that holds a subquery.
fn id_member_records(
	table: &str,
	ty: crate::ir::KeyRead,
	list: &[BoundExpr],
	policy: &DialectPolicy,
	preserve_value: bool,
) -> Result<Option<(Vec<LogicalExpr>, bool)>, PlanError> {
	let mut records = Vec::with_capacity(list.len());
	let mut has_value = false;
	for member in list {
		if let Some(key) = key_operand(member) {
			records.push(
				if preserve_value && matches!(key, crate::lower::id_key::KeyOperand::Param(_)) {
					record_or_value(table, key_read_as(ty, lower_stored(member, policy)?))
				} else {
					record_of_operand_typed(table, &key, ty)
				},
			);
		} else if is_key_or_link(member) {
			has_value = true;
			let value = key_read_as(ty, lower_stored(member, policy)?);
			records.push(if preserve_value {
				record_or_value(table, value)
			} else {
				record_of(table, value)
			});
		} else {
			return Ok(None);
		}
	}
	Ok(Some((records, has_value)))
}

/// Whether `col` is the `id` of the table the statement reads (or of the leftmost table of its
/// joins), whose record is never absent: the only `id` a lookup can read by.
fn names_scanned_record(col: &BoundColumnRef) -> bool {
	col.relation_index == 0 && col.correlation_depth == 0
}

/// The lookup form of `expr`, a top-level conjunct of a `WHERE` predicate that compares the `id`
/// of a base table, or a link column, with keys: `id = <key>` or `id IN (<keys>)`, where a key is
/// an integer or string literal, a parameter, or a column or expression that holds the key or a
/// record link.
///
/// The form compares the stored record id with the records the keys name, plainly, with no SQL
/// null handling: it is false where SQL says NULL (a NULL, absent or empty key names no record),
/// which a predicate judged for truth alone filters the same way. It is formed only for the `id`
/// or the link column of the table read, never for those of a joined table, which an outer join may
/// leave absent. A comparison anywhere else is lowered with its null handling.
pub(crate) fn lower_lookup_predicate(
	expr: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<Option<LogicalExpr>, PlanError> {
	if let BoundExpr::Binary {
		left,
		op: BoundBinaryOp::Equal,
		right,
	} = expr
	{
		if same_table_pair(left, right).is_some() {
			return Ok(None);
		}
		let (col, table, other) = match (id_column(left), id_column(right)) {
			(Some((col, table)), None) => (col, table, right.as_ref()),
			(None, Some((col, table))) => (col, table, left.as_ref()),
			(Some(_), Some(_)) => return Ok(None),
			(None, None) => return lower_link_lookup(left, right, policy),
		};
		if !names_scanned_record(col) {
			return Ok(None);
		}
		if let Some(key) = key_operand(other) {
			Ok(Some(equal(
				lower_column_raw(col),
				record_of_operand_typed(table, &key, col.key_read),
			)))
		} else if is_key_or_link(other) {
			let record = record_of(table, key_read_as(col.key_read, lower_stored(other, policy)?));
			Ok(Some(equal(lower_column_raw(col), record)))
		} else {
			Ok(None)
		}
	} else if let BoundExpr::InList {
		expr,
		list,
		negated: false,
	} = expr
	{
		if let Some((col, table)) = link_column(expr) {
			if !names_scanned_record(col) {
				return Ok(None);
			}
			let mut records = Vec::with_capacity(list.len());
			for member in list {
				let LinkOperand::Record(record) =
					link_operand(table, col.key_read, member, policy)?
				else {
					return Ok(None);
				};
				records.push(record);
			}
			let dynamic = records.iter().any(|record| !matches!(record, LogicalExpr::RecordId(_)));
			return Ok(one_or_inside(col, records).map(|predicate| {
				if dynamic {
					predicate.and(is_not_null_expr(lower_column_raw(col)))
				} else {
					predicate
				}
			}));
		}
		let Some((col, table)) = id_column(expr) else {
			return Ok(None);
		};
		if !names_scanned_record(col) {
			return Ok(None);
		}
		let Some((records, _)) = id_member_records(table, col.key_read, list, policy, false)?
		else {
			return Ok(None);
		};
		Ok(one_or_inside(col, records))
	} else {
		Ok(None)
	}
}

/// The lookup of a link column compared for equality with a value that is not an `id` column.
fn lower_link_lookup(
	left: &BoundExpr,
	right: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<Option<LogicalExpr>, PlanError> {
	let (col, table, other) = match (link_column(left), link_column(right)) {
		(Some((col, table)), None) => (col, table, right),
		(None, Some((col, table))) => (col, table, left),
		_ => return Ok(None),
	};
	if !names_scanned_record(col) {
		return Ok(None);
	}
	Ok(match link_operand(table, col.key_read, other, policy)? {
		LinkOperand::Record(record) => {
			let dynamic = !matches!(record, LogicalExpr::RecordId(_));
			let predicate = equal(lower_column_raw(col), record);
			Some(if dynamic {
				predicate.and(is_not_null_expr(lower_column_raw(col)))
			} else {
				predicate
			})
		}
		LinkOperand::Subquery(_) | LinkOperand::Plain(_) => None,
	})
}

/// The equality of `col` with the only record of `records`, or its membership of them.
fn one_or_inside(col: &BoundColumnRef, mut records: Vec<LogicalExpr>) -> Option<LogicalExpr> {
	match records.len() {
		0 => None,
		1 => records.pop().map(|record| equal(lower_column_raw(col), record)),
		_ => Some(LogicalExpr::Binary {
			left: Box::new(lower_column_raw(col)),
			op: BinaryOperator::Inside,
			right: Box::new(LogicalExpr::Array(records)),
		}),
	}
}

/// Membership of the link column `col` to `table` in an IN or NOT IN list: each member is the
/// record it names, so the comparison is made on the
/// stored links. A list with a member that holds a subquery cannot be converted, and compares the
/// key of the column with its members as they are.
fn lower_link_membership(
	col: &BoundColumnRef,
	table: &str,
	list: &[BoundExpr],
	negated: bool,
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let mut records = Vec::with_capacity(list.len());
	for member in list {
		match link_comparison_operand(table, col.key_read, member, policy)? {
			LinkOperand::Record(record) | LinkOperand::Plain(record) => records.push(record),
			LinkOperand::Subquery(_) => {
				let members = list
					.iter()
					.map(|member| lower_bound_expr(member, policy))
					.collect::<Result<Vec<_>, _>>()?;
				return Ok(sql_membership(
					key_of(lower_column_raw(col), true),
					LogicalExpr::Array(members),
					negated,
				));
			}
		}
	}
	Ok(sql_membership(lower_column_raw(col), LogicalExpr::Array(records), negated))
}

/// Lower the operands of a comparison of two values for equality or distinctness.
///
/// Two `id` columns of the same base table are compared as the stored record ids, which is the same
/// comparison as of their keys. The `id` of a base table and a column or an expression are
/// compared as the stored record id and the record of the table the value names, which is the
/// record the key names when the value holds a key and the record the link names when it holds one.
///
/// A link column is compared as stored with the `id` of its target table and with a link column to
/// the same table. Compared with anything else it is compared with the record of its target table
/// the other operand names, and with the key of the link when the other operand holds a subquery.
fn lower_operands(
	left: &BoundExpr,
	op: BoundBinaryOp,
	right: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<(LogicalExpr, LogicalExpr), PlanError> {
	if matches!(
		op,
		BoundBinaryOp::Equal
			| BoundBinaryOp::NotEqual
			| BoundBinaryOp::NullSafeEqual
			| BoundBinaryOp::DistinctFrom
	) {
		if let Some((l, r)) = same_table_pair(left, right) {
			return Ok((lower_column_raw(l), lower_column_raw(r)));
		}
		match (id_column(left), id_column(right)) {
			(Some((l, l_table)), Some((r, r_table)))
				if l_table == r_table
					&& matches!(op, BoundBinaryOp::Equal | BoundBinaryOp::NotEqual) =>
			{
				return Ok((lower_column_raw(l), lower_column_raw(r)));
			}
			(Some((col, table)), None) if is_key_or_link(right) => {
				let record =
					record_or_value(table, key_read_as(col.key_read, lower_stored(right, policy)?));
				return Ok((lower_column_raw(col), record));
			}
			(None, Some((col, table))) if is_key_or_link(left) => {
				let record =
					record_or_value(table, key_read_as(col.key_read, lower_stored(left, policy)?));
				return Ok((record, lower_column_raw(col)));
			}
			(None, None) => {
				if let Some((col, table)) = link_column(left) {
					return link_comparison(col, table, right, policy);
				}
				if let Some((col, table)) = link_column(right) {
					let (column, operand) = link_comparison(col, table, left, policy)?;
					return Ok((operand, column));
				}
			}
			_ => {}
		}
	}
	let (mut l, mut r) = (lower_bound_expr(left, policy)?, lower_bound_expr(right, policy)?);
	// A set of labels equals the text of its members, in any order.
	if matches!(
		op,
		BoundBinaryOp::Equal
			| BoundBinaryOp::NotEqual
			| BoundBinaryOp::NullSafeEqual
			| BoundBinaryOp::DistinctFrom
	) {
		if is_label_set(left)
			&& let BoundExpr::Literal(BoundLiteral::String(text)) = right
		{
			r = label_set_literal(text);
		} else if is_label_set(right)
			&& let BoundExpr::Literal(BoundLiteral::String(text)) = left
		{
			l = label_set_literal(text);
		}
	}
	if matches!(op, BoundBinaryOp::Equal | BoundBinaryOp::NotEqual) {
		if let Some(truth) = pg_bool_spelling(right) {
			r = bool_if_boolean(&l, truth, r);
		} else if let Some(truth) = pg_bool_spelling(left) {
			l = bool_if_boolean(&r, truth, l);
		}
	}
	if matches!(
		op,
		BoundBinaryOp::LessThan
			| BoundBinaryOp::LessThanEqual
			| BoundBinaryOp::GreaterThan
			| BoundBinaryOp::GreaterThanEqual
	) && let Some(reference) = scalar_enum(left).or_else(|| scalar_enum(right))
	{
		return Ok((rank(reference, l), rank(reference, r)));
	}
	if is_network(left) || is_network(right) {
		let ordering = matches!(
			op,
			BoundBinaryOp::LessThan
				| BoundBinaryOp::LessThanEqual
				| BoundBinaryOp::GreaterThan
				| BoundBinaryOp::GreaterThanEqual
		);
		let equality = matches!(
			op,
			BoundBinaryOp::Equal
				| BoundBinaryOp::NotEqual
				| BoundBinaryOp::NullSafeEqual
				| BoundBinaryOp::DistinctFrom
		);
		// Two network values compare by the network each holds, so an `inet` and a `cidr`
		// spelling the same network are equal although their stored texts differ.
		if (ordering || equality) && is_network(left) && is_network(right) {
			return Ok((network_key(l), network_key(r)));
		}
		// A network has no comparison with an operand of another declared type, such as a column
		// declared `text` or a number.
		if ordering || equality {
			let (network, other) = if is_network(left) {
				(left, right)
			} else {
				(right, left)
			};
			if !is_network(other)
				&& let Some(other_type) = declared_non_network_type(other)
			{
				let network_type =
					crate::lower::network_rank::network_type_name(network).unwrap_or("inet");
				return Err(PlanError::UndefinedOperator(format!(
					"operator does not exist: {network_type} {} {other_type}",
					comparison_symbol(op)
				)));
			}
		}
		// A literal or a parameter opposite a network is read as a value of that network's type,
		// so text the type refuses, such as a `cidr` with host bits set, is refused. A stored
		// network is its canonical text, so an equality compares that text.
		if ordering || equality {
			if let Some(text) = canonical_network_literal(left, right) {
				r = text?;
			} else if let Some(text) = canonical_network_literal(right, left) {
				l = text?;
			} else if let Some(text) = canonical_network_param(left, right, &r) {
				r = text;
			} else if let Some(text) = canonical_network_param(right, left, &l) {
				l = text;
			}
		}
		if ordering {
			return Ok((network_key(l), network_key(r)));
		}
	}
	Ok((l, r))
}

/// The name of the type `expr` is declared as when it is declared as something other than a
/// network: a number, boolean or numeric literal, a cast to a type that is not a network, or a
/// column with a PostgreSQL `SQL TYPE` naming a built-in type that is not a network. `None` for an
/// expression of no declared type, such as a string literal, a parameter or a schemaless field.
fn declared_non_network_type(expr: &BoundExpr) -> Option<String> {
	let named = |ty: SqlType| {
		(!matches!(ty, SqlType::Inet | SqlType::Cidr | SqlType::Unknown))
			.then(|| ty.name())
			.flatten()
	};
	if let BoundExpr::Literal(literal) = expr {
		return match literal {
			BoundLiteral::Integer(_) | BoundLiteral::Unsigned(_) => Some("integer".to_owned()),
			BoundLiteral::Float(_) => Some("double precision".to_owned()),
			BoundLiteral::Numeric(_) => Some("numeric".to_owned()),
			BoundLiteral::Boolean(_) => Some("boolean".to_owned()),
			BoundLiteral::String(_) | BoundLiteral::Null => None,
		};
	}
	if let BoundExpr::Cast {
		target_type,
		..
	} = expr
	{
		return named(SqlType::from_name(target_type));
	}
	if let BoundExpr::Column(col) = expr
		&& let Some(declared) = col.declared_sql.as_deref()
		&& declared.dialect()
			== surrealdb_expr::expr::statements::define::SqlTypeDialect::PostgreSql
	{
		return named(SqlType::from_name(declared.base()));
	}
	None
}

/// Refuses a predicate over `network` one of whose `peers` is declared as a type that is not a
/// network, as PostgreSQL finds no operator between them.
fn refuse_non_network_peers<'e>(
	network: &BoundExpr,
	peers: impl IntoIterator<Item = &'e BoundExpr>,
	symbol: &str,
) -> Result<(), PlanError> {
	let network_type = crate::lower::network_rank::network_type_name(network).unwrap_or("inet");
	for peer in peers {
		if !is_network(peer)
			&& let Some(other_type) = declared_non_network_type(peer)
		{
			return Err(PlanError::UndefinedOperator(format!(
				"operator does not exist: {network_type} {symbol} {other_type}"
			)));
		}
	}
	Ok(())
}

/// The SQL spelling of a comparison operator, for an error message.
fn comparison_symbol(op: BoundBinaryOp) -> &'static str {
	if op == BoundBinaryOp::Equal || op == BoundBinaryOp::NullSafeEqual {
		"="
	} else if op == BoundBinaryOp::NotEqual || op == BoundBinaryOp::DistinctFrom {
		"<>"
	} else if op == BoundBinaryOp::LessThan {
		"<"
	} else if op == BoundBinaryOp::LessThanEqual {
		"<="
	} else if op == BoundBinaryOp::GreaterThan {
		">"
	} else {
		">="
	}
}

/// `operand` read as a value of the type of `network`: a literal or a parameter as the canonical
/// text of the network it spells, refused when the type refuses it, such as a `cidr` with host
/// bits set; any other operand as `lowered`.
fn network_operand(
	network: &BoundExpr,
	operand: &BoundExpr,
	lowered: LogicalExpr,
) -> Result<LogicalExpr, PlanError> {
	if let Some(text) = canonical_network_literal(network, operand) {
		return text;
	}
	Ok(canonical_network_param(network, operand, &lowered).unwrap_or(lowered))
}

/// `subject [NOT] IN (items)` where `network`, the subject or an item, is a network: each literal
/// or parameter, the subject's included, is read as the canonical text of the network it spells.
/// When the subject is a network and every item is a literal or parameter, the stored text is
/// tested for membership; otherwise both sides are compared by their [network keys](network_key).
fn lower_network_membership(
	network: &BoundExpr,
	subject: &BoundExpr,
	value: LogicalExpr,
	list: &[BoundExpr],
	items: Vec<LogicalExpr>,
	negated: bool,
) -> Result<LogicalExpr, PlanError> {
	let value = network_operand(network, subject, value)?;
	// Only a column holds a network's canonical text; a compound subject may yield the raw text of
	// an untyped alternative, so it is compared by key.
	let all_canonical = matches!(subject, BoundExpr::Column(_))
		&& is_network(subject)
		&& list.iter().all(|item| {
			matches!(item, BoundExpr::Literal(BoundLiteral::String(_)) | BoundExpr::Param { .. })
		});
	let items = list
		.iter()
		.zip(items)
		.map(|(item, lowered)| network_operand(network, item, lowered))
		.collect::<Result<Vec<_>, _>>()?;
	if all_canonical {
		return Ok(sql_membership(value, LogicalExpr::Array(items), negated));
	}
	let keys = items.into_iter().map(network_key).collect();
	Ok(sql_membership(network_key(value), LogicalExpr::Array(keys), negated))
}

/// The truth value a string literal spells in PostgreSQL's boolean input (`'t'`, `'false'`, ...).
pub(crate) fn pg_bool_spelling(expr: &BoundExpr) -> Option<bool> {
	let BoundExpr::Literal(BoundLiteral::String(text)) = expr else {
		return None;
	};
	match text.to_ascii_lowercase().as_str() {
		"t" | "true" | "y" | "yes" | "on" => Some(true),
		"f" | "false" | "n" | "no" | "off" => Some(false),
		_ => None,
	}
}

/// What a string literal that spells a boolean is compared with `subject` as: PostgreSQL reads the
/// literal as the type of the value it is compared with, so it is that boolean where `subject` is
/// one at run time and the text otherwise.
fn bool_if_boolean(subject: &LogicalExpr, truth: bool, text: LogicalExpr) -> LogicalExpr {
	LogicalExpr::If(Box::new(LogicalIf {
		branches: vec![(
			LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal("type::is_bool".to_string()),
				args: vec![subject.clone()],
			})),
			LogicalExpr::Literal(LogicalLiteral::Bool(truth)),
		)],
		otherwise: Some(text),
	}))
}

/// The two sides of a comparison of the link column `col` to `table` with `other`: the column (or
/// its key when `other` holds a subquery) and what `other` is compared as.
fn link_comparison(
	col: &BoundColumnRef,
	table: &str,
	other: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<(LogicalExpr, LogicalExpr), PlanError> {
	Ok(match link_comparison_operand(table, col.key_read, other, policy)? {
		LinkOperand::Record(other) | LinkOperand::Plain(other) => (lower_column_raw(col), other),
		LinkOperand::Subquery(other) => (key_of(lower_column_raw(col), true), other),
	})
}

/// The operands of a MySQL arithmetic expression with each narrow column cast to the width MySQL
/// computes in, or `None` when no operand is one: an integer column is read as `BIGINT` and a
/// single-precision one as `DOUBLE`, so the sum of two `INT` values does not overflow at 32 bits
/// nor two `FLOAT` values round to 24 bits. Kept apart so that the frame of the caller stays small.
#[inline(never)]
fn mysql_widened_operands(
	left: &BoundExpr,
	op: BoundBinaryOp,
	right: &BoundExpr,
	policy: &DialectPolicy,
) -> Option<(BoundExpr, BoundExpr)> {
	if policy.dialect != crate::dialect::SqlDialect::MySql
		|| !matches!(
			op,
			BoundBinaryOp::Add
				| BoundBinaryOp::Subtract
				| BoundBinaryOp::Multiply
				| BoundBinaryOp::Divide
				| BoundBinaryOp::IntDiv
				| BoundBinaryOp::Remainder
		) {
		return None;
	}
	let widen = |operand: &BoundExpr| -> Option<BoundExpr> {
		let BoundExpr::Column(column) = operand else {
			return None;
		};
		let target_type =
			if matches!(column.declared_type, crate::SqlType::Int2 | crate::SqlType::Int4) {
				"bigint"
			} else if column.declared_type == crate::SqlType::Float4 {
				"double precision"
			} else {
				return None;
			};
		Some(BoundExpr::Cast {
			expr: Box::new(operand.clone()),
			target_type: target_type.to_owned(),
		})
	};
	let (widened_left, widened_right) = (widen(left), widen(right));
	if widened_left.is_none() && widened_right.is_none() {
		return None;
	}
	Some((
		widened_left.unwrap_or_else(|| left.clone()),
		widened_right.unwrap_or_else(|| right.clone()),
	))
}

/// Lower binary operations preserving SQL 3VL, NULL propagation, and dialect rules.
fn lower_binary_op(
	left: &BoundExpr,
	op: BoundBinaryOp,
	right: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	if matches!(
		op,
		BoundBinaryOp::JsonExtract
			| BoundBinaryOp::JsonExtractText
			| BoundBinaryOp::JsonPathExtract
			| BoundBinaryOp::JsonPathExtractText
			| BoundBinaryOp::JsonContains
	) {
		return lower_json_op(left, op, right, policy);
	}
	if let BoundBinaryOp::VectorDistance(metric) = op {
		return Ok(super::vector_expr::lower_distance(
			metric,
			lower_bound_expr(left, policy)?,
			lower_bound_expr(right, policy)?,
		));
	}
	if let Some((promoted_left, promoted_right)) =
		super::temporal::promoted_operands(left, op, right, policy.dialect)
	{
		return lower_binary_op(&promoted_left, op, &promoted_right, policy);
	}
	let (l, r) = lower_operands(left, op, right, policy)?;

	match op {
		BoundBinaryOp::NullSafeEqual => {
			// (l IS NULL AND r IS NULL) OR (l IS NOT NULL AND r IS NOT NULL AND l = r)
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![
					(
						is_null_expr(l.clone()).and(is_null_expr(r.clone())),
						LogicalExpr::Literal(LogicalLiteral::Bool(true)),
					),
					(
						is_null_expr(l.clone()).or(is_null_expr(r.clone())),
						LogicalExpr::Literal(LogicalLiteral::Bool(false)),
					),
				],
				otherwise: Some(LogicalExpr::Binary {
					left: Box::new(l),
					op: BinaryOperator::Equal,
					right: Box::new(r),
				}),
			})))
		}
		BoundBinaryOp::DistinctFrom => {
			// (l IS NULL AND r IS NULL) -> false; (one IS NULL) -> true; else l != r
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![
					(
						is_null_expr(l.clone()).and(is_null_expr(r.clone())),
						LogicalExpr::Literal(LogicalLiteral::Bool(false)),
					),
					(
						is_null_expr(l.clone()).or(is_null_expr(r.clone())),
						LogicalExpr::Literal(LogicalLiteral::Bool(true)),
					),
				],
				otherwise: Some(LogicalExpr::Binary {
					left: Box::new(l),
					op: BinaryOperator::NotEqual,
					right: Box::new(r),
				}),
			})))
		}
		BoundBinaryOp::And => {
			// 3VL AND:
			// If either is false -> false
			// If both are true -> true
			// Else -> NULL
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![
					(
						is_false_expr(l.clone()).or(is_false_expr(r.clone())),
						LogicalExpr::Literal(LogicalLiteral::Bool(false)),
					),
					(
						is_true_expr(l).and(is_true_expr(r)),
						LogicalExpr::Literal(LogicalLiteral::Bool(true)),
					),
				],
				otherwise: Some(LogicalExpr::Literal(LogicalLiteral::Null)),
			})))
		}
		BoundBinaryOp::Or => {
			// 3VL OR:
			// If either is true -> true
			// If both are false -> false
			// Else -> NULL
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![
					(
						is_true_expr(l.clone()).or(is_true_expr(r.clone())),
						LogicalExpr::Literal(LogicalLiteral::Bool(true)),
					),
					(
						is_false_expr(l).and(is_false_expr(r)),
						LogicalExpr::Literal(LogicalLiteral::Bool(false)),
					),
				],
				otherwise: Some(LogicalExpr::Literal(LogicalLiteral::Null)),
			})))
		}
		BoundBinaryOp::Concat => {
			// NULL || 'a' -> NULL
			let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
			let concat_expr = LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal("string::concat".to_string()),
				args: vec![
					LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("type::string".to_string()),
						args: vec![l],
					})),
					LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("type::string".to_string()),
						args: vec![r],
					})),
				],
			}));
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(concat_expr),
			})))
		}
		BoundBinaryOp::Like => {
			let case_insensitive = policy.dialect == SqlDialect::MySql;
			let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
			let match_call = if let BoundExpr::Literal(BoundLiteral::String(pat)) = right {
				let regex_str = sql_like_to_regex(pat, case_insensitive);
				LogicalExpr::Call(Box::new(LogicalCall {
					receiver: Function::Normal("string::matches".to_string()),
					args: vec![
						l,
						LogicalExpr::Literal(LogicalLiteral::String(Strand::from(
							regex_str.as_str(),
						))),
					],
				}))
			} else {
				LogicalExpr::Call(Box::new(LogicalCall {
					receiver: Function::Normal("string::matches".to_string()),
					args: vec![l, like_pattern_regex(r, case_insensitive)?],
				}))
			};
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(match_call),
			})))
		}
		BoundBinaryOp::ILike => {
			let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
			let match_call = if let BoundExpr::Literal(BoundLiteral::String(pat)) = right {
				let regex_str = sql_like_to_regex(pat, true);
				LogicalExpr::Call(Box::new(LogicalCall {
					receiver: Function::Normal("string::matches".to_string()),
					args: vec![
						l,
						LogicalExpr::Literal(LogicalLiteral::String(Strand::from(
							regex_str.as_str(),
						))),
					],
				}))
			} else {
				LogicalExpr::Call(Box::new(LogicalCall {
					receiver: Function::Normal("string::matches".to_string()),
					args: vec![l, like_pattern_regex(r, true)?],
				}))
			};
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(match_call),
			})))
		}
		BoundBinaryOp::Power => {
			// PostgreSQL's `^` is a double-precision power. A power with no real result (an
			// overflow, a negative base to a fractional power, zero to a negative power) is
			// NULL: the result is kept when `c - c = 0` holds, which NaN and the infinities fail.
			let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
			let as_float = |operand: LogicalExpr| LogicalExpr::Prefix {
				op: PrefixOperator::Cast(Kind::Float),
				expr: Box::new(operand),
			};
			let power = LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal("math::pow".to_string()),
				args: vec![as_float(l), as_float(r)],
			}));
			let is_finite = LogicalExpr::Binary {
				left: Box::new(LogicalExpr::Binary {
					left: Box::new(LogicalExpr::Current),
					op: BinaryOperator::Subtract,
					right: Box::new(LogicalExpr::Current),
				}),
				op: BinaryOperator::Equal,
				right: Box::new(LogicalExpr::Literal(LogicalLiteral::Float(0.0))),
			};
			let finite_power = LogicalExpr::Optional {
				base: Box::new(power),
				then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
					branches: vec![(is_finite, LogicalExpr::Current)],
					otherwise: Some(LogicalExpr::Literal(LogicalLiteral::Null)),
				}))),
			};
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(finite_power),
			})))
		}
		BoundBinaryOp::IntDiv => {
			let null_or_zero =
				is_zero_expr(r.clone()).or(is_null_expr(l.clone())).or(is_null_expr(r.clone()));
			let div_expr = LogicalExpr::Binary {
				left: Box::new(l),
				op: BinaryOperator::Divide,
				right: Box::new(r),
			};
			let floor_expr = LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal("math::floor".to_string()),
				args: vec![div_expr],
			}));
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_or_zero, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(floor_expr),
			})))
		}
		BoundBinaryOp::Divide => {
			if policy.dialect == SqlDialect::Postgres {
				// In PostgreSQL: division by zero throws an error; NULL propagates
				let zero_check = is_zero_expr(r.clone());
				let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
				Ok(LogicalExpr::If(Box::new(LogicalIf {
					branches: vec![
						(
							zero_check,
							LogicalExpr::Throw(Box::new(LogicalExpr::Literal(
								LogicalLiteral::String(Strand::from("division by zero")),
							))),
						),
						(null_check, LogicalExpr::Literal(LogicalLiteral::Null)),
					],
					otherwise: Some(LogicalExpr::Binary {
						left: Box::new(l),
						op: BinaryOperator::Divide,
						right: Box::new(r),
					}),
				})))
			} else {
				// In MySQL: division by zero returns NULL; NULL propagates
				let null_or_zero =
					is_zero_expr(r.clone()).or(is_null_expr(l.clone())).or(is_null_expr(r.clone()));
				Ok(LogicalExpr::If(Box::new(LogicalIf {
					branches: vec![(null_or_zero, LogicalExpr::Literal(LogicalLiteral::Null))],
					otherwise: Some(LogicalExpr::Binary {
						left: Box::new(l),
						op: BinaryOperator::Divide,
						right: Box::new(r),
					}),
				})))
			}
		}
		BoundBinaryOp::Add => Ok(null_propagating_binary(l, r, BinaryOperator::Add)),
		BoundBinaryOp::Subtract => Ok(null_propagating_binary(l, r, BinaryOperator::Subtract)),
		BoundBinaryOp::Multiply => Ok(null_propagating_binary(l, r, BinaryOperator::Multiply)),
		BoundBinaryOp::Remainder => Ok(null_propagating_binary(l, r, BinaryOperator::Remainder)),
		BoundBinaryOp::Equal => Ok(null_propagating_binary(l, r, BinaryOperator::Equal)),
		BoundBinaryOp::NotEqual => Ok(null_propagating_binary(l, r, BinaryOperator::NotEqual)),
		BoundBinaryOp::LessThan => Ok(null_propagating_binary(l, r, BinaryOperator::LessThan)),
		BoundBinaryOp::LessThanEqual => {
			Ok(null_propagating_binary(l, r, BinaryOperator::LessThanEqual))
		}
		BoundBinaryOp::GreaterThan => Ok(null_propagating_binary(l, r, BinaryOperator::MoreThan)),
		BoundBinaryOp::GreaterThanEqual => {
			Ok(null_propagating_binary(l, r, BinaryOperator::MoreThanEqual))
		}
		BoundBinaryOp::JsonExtract
		| BoundBinaryOp::JsonExtractText
		| BoundBinaryOp::JsonPathExtract
		| BoundBinaryOp::JsonPathExtractText
		| BoundBinaryOp::JsonContains
		| BoundBinaryOp::VectorDistance(_)
		| BoundBinaryOp::Regexp
		| BoundBinaryOp::RegexpCi => unreachable!(),
	}
}

fn null_propagating_binary(l: LogicalExpr, r: LogicalExpr, op: BinaryOperator) -> LogicalExpr {
	let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
	LogicalExpr::If(Box::new(LogicalIf {
		branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
		otherwise: Some(LogicalExpr::Binary {
			left: Box::new(l),
			op,
			right: Box::new(r),
		}),
	}))
}

/// The most steps a `#>>` path may take: each step is one level of the lowered expression, so a
/// path from a client is bounded rather than lowered to any depth.
const MAX_JSON_PATH_STEPS: usize = 64;

/// Lower `->`, `->>`, `#>>` and `@>`.
///
/// `->` gives the value it reaches with an absent one read as NULL, and `->>` and `#>>` give its
/// text. The operators of a chain such as `doc -> 'a' -> 'b' ->> 'c'` are read as one place, so
/// only the outermost converts what it reaches.
fn lower_json_op(
	left: &BoundExpr,
	op: BoundBinaryOp,
	right: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	if op == BoundBinaryOp::JsonContains {
		if is_range(left) && !is_range(right) {
			// A range contains an element when the element lies between its bounds.
			return Ok(LogicalExpr::Binary {
				left: Box::new(lower_bound_expr(left, policy)?),
				op: BinaryOperator::Contain,
				right: Box::new(lower_bound_expr(right, policy)?),
			});
		}
		return Err(PlanError::UnsupportedExpression("the @> containment operator".into()));
	}
	let Some(place) = lower_json_place(left, op, right, policy)? else {
		return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
	};
	Ok(if matches!(op, BoundBinaryOp::JsonExtract | BoundBinaryOp::JsonPathExtract) {
		absent_as_null(place)
	} else {
		json_text(place)
	})
}

/// Whether `expr` is a range. A multirange is not a range.
pub(crate) fn is_range(expr: &BoundExpr) -> bool {
	range_type(expr).is_some()
}

/// The range type of `expr`, when it is known to be a range: a column declared as a range type, a
/// cast to one, or a `COALESCE`, `NULLIF`, `GREATEST`, `LEAST` or `CASE` whose range results
/// share one [range type](common_range_type).
pub(crate) fn range_type(expr: &BoundExpr) -> Option<crate::SqlType> {
	if let BoundExpr::Column(col) = expr {
		return matches!(col.declared_type, crate::SqlType::Range(_)).then_some(col.declared_type);
	}
	if let BoundExpr::Cast {
		target_type,
		..
	} = expr
	{
		let ty = crate::SqlType::from_name(target_type);
		return matches!(ty, crate::SqlType::Range(_)).then_some(ty);
	}
	if let BoundExpr::FunctionCall {
		name,
		args,
		..
	} = expr
		&& matches!(name.as_str(), "coalesce" | "nullif" | "greatest" | "least")
	{
		return common_range_type(args.iter());
	}
	if let BoundExpr::Case {
		branches,
		else_expr,
		..
	} = expr
	{
		return common_range_type(
			branches.iter().map(|(_, result)| result).chain(else_expr.as_deref()),
		);
	}
	None
}

/// The range type every range among `results` has. `None` when none is a range, when two are
/// ranges of different types, or when a result is of a type of its own that is not a range:
/// PostgreSQL resolves those to no common type. A string literal, `NULL` or parameter is of no
/// type of its own and takes the range type.
fn common_range_type<'e>(results: impl Iterator<Item = &'e BoundExpr>) -> Option<crate::SqlType> {
	let mut common = None;
	for result in results {
		if is_untyped(result) {
			continue;
		}
		let ty = range_type(result)?;
		if common.is_some_and(|common| common != ty) {
			return None;
		}
		common = Some(ty);
	}
	common
}

/// Whether `expr` is of no type of its own, which PostgreSQL resolves from the operands beside it:
/// a string literal, `NULL` or a parameter.
pub(crate) fn is_untyped(expr: &BoundExpr) -> bool {
	matches!(
		expr,
		BoundExpr::Literal(BoundLiteral::String(_) | BoundLiteral::Null) | BoundExpr::Param { .. }
	)
}

/// `operand IS NULL`.
///
/// The test holds for NONE as it does for NULL, and a JSON operator is NULL or NONE exactly when
/// the place it reaches is, so a JSON operator is tested at that place. Neither the NONE read as
/// NULL nor the text of what it reaches is of use to the test. A [place](is_json_place) is read
/// by each of the two comparisons, and any other value is evaluated once.
fn lower_null_test(operand: &BoundExpr, policy: &DialectPolicy) -> Result<LogicalExpr, PlanError> {
	let BoundExpr::Binary {
		left,
		op:
			op @ (BoundBinaryOp::JsonExtract
			| BoundBinaryOp::JsonExtractText
			| BoundBinaryOp::JsonPathExtract
			| BoundBinaryOp::JsonPathExtractText),
		right,
	} = operand
	else {
		return Ok(is_null_expr(lower_stored(operand, policy)?));
	};
	Ok(match lower_json_place(left, *op, right, policy)? {
		Some(place) if is_json_place(&place) => is_null_expr(place),
		Some(value) => value_is_null(value),
		None => LogicalExpr::Literal(LogicalLiteral::Bool(true)),
	})
}

/// Whether `value` is NULL or NONE, with `value` evaluated once: an optional step gives NONE or
/// NULL for either, and `false` for any other value.
fn value_is_null(value: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(LogicalExpr::Optional {
			base: Box::new(value),
			then: Box::new(LogicalExpr::Literal(LogicalLiteral::Bool(false))),
		}),
		op: BinaryOperator::NullCoalescing,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Bool(true))),
	}
}

/// The value `->`, `->>` or `#>>` reaches, read where it lies: NONE when it reaches nothing,
/// and `None` when the operator is NULL whatever the value is.
///
/// The place is neither read as NULL nor converted to text, so that a consumer that is not
/// interested in either, and a step that continues from it, read it without copying it.
fn lower_json_place(
	left: &BoundExpr,
	op: BoundBinaryOp,
	right: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<Option<LogicalExpr>, PlanError> {
	let base = lower_json_base(left, policy)?;
	if matches!(op, BoundBinaryOp::JsonPathExtract | BoundBinaryOp::JsonPathExtractText) {
		let BoundExpr::Literal(BoundLiteral::String(path)) = right else {
			return Err(PlanError::UnsupportedExpression(
				"the path of #> and #>> must be a text array literal such as '{a,b}'".into(),
			));
		};
		let elements = parse_text_array_literal(path).ok_or_else(|| {
			PlanError::InvalidTextRepresentation(format!("malformed array literal: \"{path}\""))
		})?;
		// A path with a NULL element reaches nothing.
		let Some(steps) = elements.into_iter().collect::<Option<Vec<String>>>() else {
			return Ok(None);
		};
		if steps.len() > MAX_JSON_PATH_STEPS {
			return Err(PlanError::UnsupportedExpression(format!(
				"the path of #> or #>> has more than {MAX_JSON_PATH_STEPS} steps"
			)));
		}
		let mut two_sided_steps = 0;
		return Ok(base.map(|base| {
			steps.iter().fold(base, |base, step| json_path_step(base, step, &mut two_sided_steps))
		}));
	}
	let operand = json_operand(op, right)?;
	let Some(base) = base else {
		return Ok(None);
	};
	Ok(match operand {
		JsonOperand::Null => None,
		JsonOperand::Key(key) => Some(json_member(base, &key)),
		JsonOperand::Element(index) => Some(json_element(base, index)),
		JsonOperand::Dynamic => Some(LogicalExpr::Index {
			base: Box::new(base),
			index: Box::new(lower_bound_expr(right, policy)?),
		}),
	})
}

/// The value a JSON operator takes its steps from. A `->` is the place it reaches, so a chain of
/// them is one place; `None` is a value that is NULL.
fn lower_json_base(
	left: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<Option<LogicalExpr>, PlanError> {
	if let BoundExpr::Binary {
		left,
		op: BoundBinaryOp::JsonExtract,
		right,
	} = left
	{
		lower_json_place(left, BoundBinaryOp::JsonExtract, right, policy)
	} else {
		lower_bound_expr(left, policy).map(Some)
	}
}

/// The right operand of `->` and `->>`, by the step it takes.
enum JsonOperand {
	/// A text literal: names a member of an object.
	Key(String),
	/// An integer literal: selects an element of an array, counting from the end when negative.
	Element(i64),
	/// A NULL literal: the result is NULL.
	Null,
	/// Any other operand, used as the index of the value as it evaluates: text names a member of
	/// an object and nothing else, and an integer is an element of an array counted from the
	/// start, or the member of an object that is named by the same text.
	Dynamic,
}

/// The step `->` or `->>` takes for `operand`.
///
/// PostgreSQL defines the operators for a text key and for an `integer` index. A literal of any
/// other type, which is an integer that does not fit 32 bits, a number with a fraction or a
/// boolean, has no operator and is refused.
fn json_operand(op: BoundBinaryOp, operand: &BoundExpr) -> Result<JsonOperand, PlanError> {
	let (negated, literal) = if let BoundExpr::Literal(literal) = operand {
		(false, literal)
	} else if let BoundExpr::Unary {
		op: BoundUnaryOp::Negate,
		expr,
	} = operand
		&& let BoundExpr::Literal(literal) = &**expr
	{
		(true, literal)
	} else {
		return Ok(JsonOperand::Dynamic);
	};
	let undefined = |operand_type: &str| {
		let operator = if op == BoundBinaryOp::JsonExtract {
			"->"
		} else {
			"->>"
		};
		PlanError::UndefinedOperator(format!("jsonb {operator} {operand_type}"))
	};
	match literal {
		BoundLiteral::String(key) if !negated => Ok(JsonOperand::Key(key.clone())),
		BoundLiteral::Null if !negated => Ok(JsonOperand::Null),
		BoundLiteral::Integer(index) => match i32::try_from(*index) {
			Ok(index) if negated => Ok(JsonOperand::Element(-i64::from(index))),
			Ok(index) => Ok(JsonOperand::Element(i64::from(index))),
			Err(_) => Err(undefined("bigint")),
		},
		BoundLiteral::Unsigned(_) | BoundLiteral::Float(_) | BoundLiteral::Numeric(_) => {
			Err(undefined("numeric"))
		}
		BoundLiteral::Boolean(_) => Err(undefined("boolean")),
		BoundLiteral::String(_) | BoundLiteral::Null => Ok(JsonOperand::Dynamic),
	}
}

fn json_call(name: &str, args: Vec<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal(name.to_string()),
		args,
	}))
}

fn json_integer(value: i64) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::Integer(value))
}

/// The member `key` of `base` when `base` is an object, and NONE for an array, a scalar, NULL or
/// NONE. An index by text reads an object and nothing else, so `base` is read once and is not
/// mapped over the elements of an array.
fn json_member(base: LogicalExpr, key: &str) -> LogicalExpr {
	LogicalExpr::Index {
		base: Box::new(base),
		index: Box::new(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(key)))),
	}
}

/// Whether `value` is an array: the slice `[0..0]` of an array is the empty array and of
/// anything else is NONE. The slice is read from `value` where it lies, so `value` is not copied
/// out as it is to a function.
fn json_is_array(value: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(LogicalExpr::Index {
			base: Box::new(value),
			index: Box::new(LogicalExpr::Binary {
				left: Box::new(json_integer(0)),
				op: BinaryOperator::Range,
				right: Box::new(json_integer(0)),
			}),
		}),
		op: BinaryOperator::ExactEqual,
		right: Box::new(LogicalExpr::Array(Vec::new())),
	}
}

/// The element `index` of `base` when `base` is an array, counting from the end when `index` is
/// negative: NONE for an index out of range in either direction and for a value that is not an
/// array.
///
/// An index from the start is the one-element slice `[index..index + 1]`, which is NONE unless
/// `base` is an array, and the last element is `[$]`, which is NONE unless `base` is a non-empty
/// array. Both read `base` where it lies and copy only the element. An index before the last
/// element needs the length of the array, so `base` is bound as a value and copied out whole.
fn json_element(base: LogicalExpr, index: i64) -> LogicalExpr {
	if index >= 0 {
		let Some(end) = index.checked_add(1) else {
			return LogicalExpr::Literal(LogicalLiteral::None);
		};
		return LogicalExpr::Index {
			base: Box::new(LogicalExpr::Index {
				base: Box::new(base),
				index: Box::new(LogicalExpr::Binary {
					left: Box::new(json_integer(index)),
					op: BinaryOperator::Range,
					right: Box::new(json_integer(end)),
				}),
			}),
			index: Box::new(json_integer(0)),
		};
	}
	if index == -1 {
		return LogicalExpr::Last(Box::new(base));
	}
	json_element_or(base, index, None)
}

/// The element `index` of `base` when `base` is an array, counting from the end when `index` is
/// negative, and `otherwise` for any other value, NONE when there is none. `base` is evaluated
/// once, as the value `otherwise` is over, and is copied out whole.
fn json_element_or(base: LogicalExpr, index: i64, otherwise: Option<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Optional {
		base: Box::new(base),
		then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(
				json_is_array(LogicalExpr::Current),
				json_call("array::at", vec![LogicalExpr::Current, json_integer(index)]),
			)],
			otherwise,
		}))),
	}
}

/// The array index a step of a `#>>` path spells, as PostgreSQL reads one: optional leading
/// whitespace, an optional sign and decimal digits that fit a 32-bit integer.
fn json_path_index(step: &str) -> Option<i64> {
	let step = step.trim_start_matches(is_array_space);
	let digits = step.strip_prefix(['+', '-']).unwrap_or(step);
	if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
		return None;
	}
	step.parse::<i32>().ok().map(i64::from)
}

/// How many steps of one `#>>` path read their value twice.
///
/// A step that reaches the member named by its text on an object and the element it indexes on an
/// array reads both of them from the value in place, which evaluates the value twice. Each such
/// step doubles the expression of the steps before it, so a path takes only this many, and only
/// over a [place](is_json_place); the later ones, and every one over a value that is not a place,
/// evaluate the value once and copy it out whole.
pub(crate) const MAX_TWO_SIDED_STEPS: usize = 3;

/// Whether `expr` is a place: a chain of reads from the row, a parameter or an outer row, by a
/// literal key or index. A place can be evaluated any number of times to the same value, at the
/// cost of the reads alone, so a step may read it twice. A call, a subquery or any other
/// expression may be neither deterministic nor cheap, and is evaluated once.
fn is_json_place(expr: &LogicalExpr) -> bool {
	if matches!(expr, LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Param(_)) {
		true
	} else if let LogicalExpr::Access {
		base,
		..
	}
	| LogicalExpr::Last(base) = expr
	{
		is_json_place(base)
	} else if let LogicalExpr::Index {
		base,
		index,
	} = expr
	{
		is_json_place(base) && is_literal_index(index)
	} else if let LogicalExpr::Binary {
		left,
		op: BinaryOperator::NullCoalescing,
		right,
	} = expr
	{
		// The value of a step that reads both an object and an array.
		is_json_place(left) && is_json_place(right)
	} else {
		false
	}
}

/// Whether `index` is a literal or a range of literals.
fn is_literal_index(index: &LogicalExpr) -> bool {
	if matches!(index, LogicalExpr::Literal(_)) {
		true
	} else if let LogicalExpr::Binary {
		left,
		op: BinaryOperator::Range,
		right,
	} = index
	{
		matches!(**left, LogicalExpr::Literal(_)) && matches!(**right, LogicalExpr::Literal(_))
	} else {
		false
	}
}

/// One step of a `#>>` path. Every step is text: on an object it names a member, on an array it
/// is an index when it spells an integer, and on anything else it reaches nothing.
///
/// `two_sided_steps` counts the steps of the path so far that read their value twice.
pub(crate) fn json_path_step(
	base: LogicalExpr,
	step: &str,
	two_sided_steps: &mut usize,
) -> LogicalExpr {
	let Some(index) = json_path_index(step) else {
		return json_member(base, step);
	};
	if index >= 0 && index.to_string() == step {
		// The same text names the member of an object and the element of an array.
		return LogicalExpr::Index {
			base: Box::new(base),
			index: Box::new(json_integer(index)),
		};
	}
	if index >= -1 && *two_sided_steps < MAX_TWO_SIDED_STEPS && is_json_place(&base) {
		*two_sided_steps += 1;
		// At most one side reaches a value: a text index reads an object only, and the element
		// of an array is NONE for any other value.
		return LogicalExpr::Binary {
			left: Box::new(json_member(base.clone(), step)),
			op: BinaryOperator::NullCoalescing,
			right: Box::new(json_element(base, index)),
		};
	}
	json_element_or(base, index, Some(json_member(LogicalExpr::Current, step)))
}

/// `value` with NONE read as NULL. A member that is not there and a JSON null are both NULL.
pub(crate) fn absent_as_null(value: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(value),
		op: BinaryOperator::NullCoalescing,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
	}
}

/// The text `->>` and `#>>` give an extracted JSON value: NULL when the value is absent or null,
/// a string as itself, and anything else as its JSON text. `value` is evaluated once.
pub(crate) fn json_text(value: LogicalExpr) -> LogicalExpr {
	absent_as_null(LogicalExpr::Optional {
		base: Box::new(value),
		then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(
				json_call("type::is_string", vec![LogicalExpr::Current]),
				LogicalExpr::Current,
			)],
			otherwise: Some(json_call("encoding::json::encode", vec![LogicalExpr::Current])),
		}))),
	})
}
/// Helper to create `(expr == NULL || expr == NONE)`.
pub fn is_null_expr(expr: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(LogicalExpr::Binary {
			left: Box::new(expr.clone()),
			op: BinaryOperator::Equal,
			right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
		}),
		op: BinaryOperator::Or,
		right: Box::new(LogicalExpr::Binary {
			left: Box::new(expr),
			op: BinaryOperator::Equal,
			right: Box::new(LogicalExpr::Literal(LogicalLiteral::None)),
		}),
	}
}

/// Helper to create `(expr != NULL && expr != NONE)`.
pub fn is_not_null_expr(expr: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(LogicalExpr::Binary {
			left: Box::new(expr.clone()),
			op: BinaryOperator::NotEqual,
			right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
		}),
		op: BinaryOperator::And,
		right: Box::new(LogicalExpr::Binary {
			left: Box::new(expr),
			op: BinaryOperator::NotEqual,
			right: Box::new(LogicalExpr::Literal(LogicalLiteral::None)),
		}),
	}
}

/// Helper to create `(expr == 0)`.
pub fn is_zero_expr(expr: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(expr),
		op: BinaryOperator::Equal,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(0))),
	}
}

/// Helper to create `(expr == true)`.
pub fn is_true_expr(expr: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(expr),
		op: BinaryOperator::ExactEqual,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Bool(true))),
	}
}

/// Helper to create `(expr == false)`.
pub fn is_false_expr(expr: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(expr),
		op: BinaryOperator::ExactEqual,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Bool(false))),
	}
}

trait LogicalExprCombine {
	fn and(self, other: LogicalExpr) -> LogicalExpr;
	fn or(self, other: LogicalExpr) -> LogicalExpr;
}

impl LogicalExprCombine for LogicalExpr {
	fn and(self, other: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Binary {
			left: Box::new(self),
			op: BinaryOperator::And,
			right: Box::new(other),
		}
	}
	fn or(self, other: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Binary {
			left: Box::new(self),
			op: BinaryOperator::Or,
			right: Box::new(other),
		}
	}
}

/// Lowers `left ~ right`, `left ~* right` and their negations (`!~`, `!~*`, `NOT REGEXP`): NULL
/// when either operand is NULL, otherwise whether `left` matches the pattern `right`, ignoring
/// case when `case_insensitive`.
///
/// The `i` flag leads the pattern. A pattern written as a string literal gets it at plan time, and
/// then only `left` can be NULL, so it is evaluated once and matched as the current value. A
/// pattern known only at run time gets the flag by concatenation.
fn lower_regex_match(
	left: &BoundExpr,
	right: &BoundExpr,
	case_insensitive: bool,
	negate: bool,
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let l = lower_bound_expr(left, policy)?;
	let r = lower_bound_expr(right, policy)?;
	let matches = |subject: LogicalExpr, pattern: LogicalExpr| {
		let call = LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal("string::matches".to_string()),
			args: vec![subject, pattern],
		}));
		if negate {
			LogicalExpr::Prefix {
				op: PrefixOperator::Not,
				expr: Box::new(call),
			}
		} else {
			call
		}
	};
	if matches!(right, BoundExpr::Literal(BoundLiteral::Null)) {
		Ok(LogicalExpr::Literal(LogicalLiteral::Null))
	} else if let BoundExpr::Literal(BoundLiteral::String(pattern)) = right {
		let pattern = if case_insensitive {
			Strand::from(format!("(?i){pattern}").as_str())
		} else {
			Strand::from(pattern.as_str())
		};
		Ok(LogicalExpr::Binary {
			left: Box::new(LogicalExpr::Optional {
				base: Box::new(l),
				then: Box::new(matches(
					LogicalExpr::Current,
					LogicalExpr::Literal(LogicalLiteral::String(pattern)),
				)),
			}),
			op: BinaryOperator::NullCoalescing,
			right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
		})
	} else {
		let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
		let pattern = if case_insensitive {
			LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal("string::concat".to_string()),
				args: vec![LogicalExpr::Literal(LogicalLiteral::String(Strand::from("(?i)"))), r],
			}))
		} else {
			r
		};
		Ok(LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
			otherwise: Some(matches(l, pattern)),
		})))
	}
}

/// Lower a `BoundTarget` into a `LogicalField`.
pub fn lower_bound_target(
	target: &BoundTarget,
	policy: &DialectPolicy,
) -> Result<LogicalField, PlanError> {
	if target.is_wildcard {
		return Ok(LogicalField::All);
	}
	let expr = lower_stored(&target.expr, policy)?;
	let alias = match &target.alias {
		Some(a) => Some(OutputPath::field(a.clone())),
		None => match &target.expr {
			BoundExpr::Column(col) if col.column != "*" => {
				Some(OutputPath::field(col.column.clone()))
			}
			BoundExpr::FunctionCall {
				name,
				..
			} => Some(OutputPath::field(name.clone())),
			BoundExpr::UserVar(name) => Some(OutputPath::field(format!("@{name}"))),
			BoundExpr::SystemVar {
				name,
				is_global,
			} => {
				let prefix = if *is_global {
					"@@global."
				} else {
					"@@"
				};
				Some(OutputPath::field(format!("{prefix}{name}")))
			}
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
			| BoundExpr::ArraySubquery(_)
			| BoundExpr::WindowFunction {
				..
			}
			| BoundExpr::Column(_) => None,
		},
	};
	Ok(LogicalField::user(expr, alias))
}

/// Lower a `BoundOrder` into one or more `LogicalOrder` items.
///
/// Handles dialect-specific NULL ordering:
/// - In SurrealDB: ASC has NULLs first, DESC has NULLs last.
/// - If `ASC NULLS LAST` requested (PG default): emit `(expr IS NULL) ASC, expr ASC`.
/// - If `DESC NULLS FIRST` requested (PG default): emit `(expr IS NULL) DESC, expr DESC`.
pub fn lower_bound_order(
	order: &BoundOrder,
	policy: &DialectPolicy,
) -> Result<Vec<LogicalOrder>, PlanError> {
	// The stored record ids of a table order as their keys do, so an `id` column and a link column
	// sort as stored.
	let value = match id_column(&order.expr).or_else(|| link_column(&order.expr)) {
		Some((col, _)) => lower_column_raw(col),
		// The untyped alternatives of a network compound are read as its network type, so text
		// the type refuses is refused whichever alternative a row takes.
		None if is_network(&order.expr) => lower_bound_expr(
			&crate::binder::pgsql::network_alternatives_as_network(order.expr.clone()),
			policy,
		)?,
		None => lower_bound_expr(&order.expr, policy)?,
	};
	// An enum column orders by the position of its labels in the declaration.
	let value = match scalar_enum(&order.expr) {
		Some(reference) => rank(reference, value),
		// A network column orders by address, as PostgreSQL orders `inet` and `cidr`.
		None if is_network(&order.expr) => network_key(value),
		None => value,
	};
	let direction = match order.direction {
		BoundOrderDirection::Asc => true,
		BoundOrderDirection::Desc => false,
	};
	// The test that puts the NULLs last or first, which a distance answers from its column rather
	// than from itself, so that it is computed once.
	let null_test = match super::vector_knn::null_probe(order, policy)? {
		Some(probe) => probe,
		None => is_null_expr(value.clone()),
	};

	if direction && !order.nulls_first {
		// ASC NULLS LAST: false (non-null) before true (null)
		Ok(vec![
			LogicalOrder {
				value: null_test,
				direction: true,
				collate: false,
				numeric: false,
			},
			LogicalOrder {
				value,
				direction: true,
				collate: false,
				numeric: false,
			},
		])
	} else if !direction && order.nulls_first {
		// DESC NULLS FIRST: true (null) before false (non-null)
		Ok(vec![
			LogicalOrder {
				value: null_test,
				direction: false,
				collate: false,
				numeric: false,
			},
			LogicalOrder {
				value,
				direction: false,
				collate: false,
				numeric: false,
			},
		])
	} else {
		Ok(vec![LogicalOrder {
			value,
			direction,
			collate: false,
			numeric: false,
		}])
	}
}

/// Check if a `BoundExpr` contains any aggregate function calls.
pub fn bound_expr_contains_aggregate(expr: &BoundExpr) -> bool {
	match expr {
		BoundExpr::FunctionCall {
			name,
			args,
			..
		} => {
			crate::functions::FunctionRegistry::is_bound_aggregate(name)
				|| args.iter().any(bound_expr_contains_aggregate)
		}
		BoundExpr::Binary {
			left,
			right,
			..
		} => bound_expr_contains_aggregate(left) || bound_expr_contains_aggregate(right),
		BoundExpr::Unary {
			expr,
			..
		} => bound_expr_contains_aggregate(expr),
		BoundExpr::Access {
			base,
			..
		} => bound_expr_contains_aggregate(base),
		BoundExpr::Between {
			expr,
			low,
			high,
			..
		} => {
			bound_expr_contains_aggregate(expr)
				|| bound_expr_contains_aggregate(low)
				|| bound_expr_contains_aggregate(high)
		}
		BoundExpr::InList {
			expr,
			list,
			..
		} => bound_expr_contains_aggregate(expr) || list.iter().any(bound_expr_contains_aggregate),
		BoundExpr::IsNull {
			expr,
			..
		} => bound_expr_contains_aggregate(expr),
		BoundExpr::Cast {
			expr,
			..
		} => bound_expr_contains_aggregate(expr),
		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => {
			operand.as_ref().is_some_and(|op| bound_expr_contains_aggregate(op))
				|| branches.iter().any(|(c, v)| {
					bound_expr_contains_aggregate(c) || bound_expr_contains_aggregate(v)
				}) || else_expr.as_ref().is_some_and(|el| bound_expr_contains_aggregate(el))
		}
		BoundExpr::WindowFunction {
			args,
			..
		} => args.iter().any(bound_expr_contains_aggregate),
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		}
		| BoundExpr::ScalarSubquery(_)
		| BoundExpr::Exists(_)
		| BoundExpr::InSubquery {
			..
		}
		| BoundExpr::ArraySubquery(_) => false,
	}
}

fn lower_system_var(
	name: &str,
	_is_global: bool,
	_policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let literal = match mysql_system_variable(name) {
		Some(SystemVariable::Int(value)) => LogicalLiteral::Integer(value),
		Some(SystemVariable::Flag(on)) => LogicalLiteral::Integer(i64::from(on)),
		Some(SystemVariable::Text(value)) => LogicalLiteral::String(Strand::from(value)),
		None => LogicalLiteral::Null,
	};
	Ok(LogicalExpr::Literal(literal))
}

fn current_item(index: i64) -> LogicalExpr {
	LogicalExpr::Index {
		base: Box::new(LogicalExpr::Current),
		index: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(index))),
	}
}

fn sql_membership(value: LogicalExpr, items: LogicalExpr, negated: bool) -> LogicalExpr {
	let inside = |value| LogicalExpr::Binary {
		left: Box::new(value),
		op: BinaryOperator::Inside,
		right: Box::new(current_item(1)),
	};
	LogicalExpr::Optional {
		base: Box::new(LogicalExpr::Array(vec![value, items])),
		then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![
				(
					equal(current_item(1), LogicalExpr::Array(Vec::new())),
					LogicalExpr::Literal(LogicalLiteral::Bool(negated)),
				),
				(is_null_expr(current_item(0)), LogicalExpr::Literal(LogicalLiteral::Null)),
				(inside(current_item(0)), LogicalExpr::Literal(LogicalLiteral::Bool(!negated))),
				(
					inside(LogicalExpr::Literal(LogicalLiteral::Null))
						.or(inside(LogicalExpr::Literal(LogicalLiteral::None))),
					LogicalExpr::Literal(LogicalLiteral::Null),
				),
			],
			otherwise: Some(LogicalExpr::Literal(LogicalLiteral::Bool(negated))),
		}))),
	}
}

/// `value <op> ANY|ALL (items)` for `=` (`equal`) or `<>`, three-valued as PostgreSQL has it, with
/// `value` and `items` each evaluated once.
///
/// `= ANY` and `<> ALL` look for an item equal to `value`; `= ALL` and `<> ANY` look for a
/// non-NULL item that differs from it. Finding one decides the result (`= ANY` true, `<> ALL`
/// false, `= ALL` false, `<> ANY` true). An empty array decides it the other way (`ALL` true,
/// `ANY` false). Otherwise the result is NULL when `items` is not an array, when `value` is NULL,
/// or when `items` holds a NULL, and the answer of finding nothing in the remaining cases.
fn sql_quantified(value: LogicalExpr, items: LogicalExpr, is_eq: bool, all: bool) -> LogicalExpr {
	let null = || LogicalExpr::Literal(LogicalLiteral::Null);
	let boolean = |value| LogicalExpr::Literal(LogicalLiteral::Bool(value));
	let inside = |value| LogicalExpr::Binary {
		left: Box::new(value),
		op: BinaryOperator::Inside,
		right: Box::new(current_item(1)),
	};
	let call = |name: &str, args| {
		LogicalExpr::Call(Box::new(LogicalCall {
			receiver: Function::Normal(name.to_string()),
			args,
		}))
	};
	// The witness is an item equal to `value` for `= ANY` and `<> ALL`, and a non-NULL item that
	// differs from it for `= ALL` and `<> ANY`.
	let looks_for_equal = is_eq != all;
	let found = if looks_for_equal {
		inside(current_item(0))
	} else {
		LogicalExpr::Binary {
			left: Box::new(call(
				"array::len",
				vec![call(
					"array::complement",
					vec![
						current_item(1),
						LogicalExpr::Array(vec![
							current_item(0),
							null(),
							LogicalExpr::Literal(LogicalLiteral::None),
						]),
					],
				)],
			)),
			op: BinaryOperator::MoreThan,
			right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(0))),
		}
	};
	// Whether the quantified comparison is true when a witness is found.
	let on_found = if looks_for_equal {
		is_eq
	} else {
		!is_eq
	};
	LogicalExpr::Optional {
		base: Box::new(LogicalExpr::Array(vec![value, items])),
		then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![
				(
					LogicalExpr::Prefix {
						op: PrefixOperator::Not,
						expr: Box::new(call("type::is_array", vec![current_item(1)])),
					},
					null(),
				),
				(equal(current_item(1), LogicalExpr::Array(Vec::new())), boolean(all)),
				(is_null_expr(current_item(0)), null()),
				(found, boolean(on_found)),
				(inside(null()).or(inside(LogicalExpr::Literal(LogicalLiteral::None))), null()),
			],
			otherwise: Some(boolean(!on_found)),
		}))),
	}
}

fn sql_between(
	value: LogicalExpr,
	low: LogicalExpr,
	high: LogicalExpr,
	negated: bool,
) -> LogicalExpr {
	let comparison = |op, bound: i64| {
		LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![(
				is_null_expr(current_item(0)).or(is_null_expr(current_item(bound))),
				LogicalExpr::Literal(LogicalLiteral::Null),
			)],
			otherwise: Some(LogicalExpr::Binary {
				left: Box::new(current_item(0)),
				op,
				right: Box::new(current_item(bound)),
			}),
		}))
	};
	let ge = comparison(BinaryOperator::MoreThanEqual, 1);
	let le = comparison(BinaryOperator::LessThanEqual, 2);
	LogicalExpr::Optional {
		base: Box::new(LogicalExpr::Array(vec![value, low, high])),
		then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
			branches: vec![
				(
					is_false_expr(ge.clone()).or(is_false_expr(le.clone())),
					LogicalExpr::Literal(LogicalLiteral::Bool(negated)),
				),
				(
					is_true_expr(ge).and(is_true_expr(le)),
					LogicalExpr::Literal(LogicalLiteral::Bool(!negated)),
				),
			],
			otherwise: Some(LogicalExpr::Literal(LogicalLiteral::Null)),
		}))),
	}
}

/// The PostgreSQL types a catalog value can name: `(oid, typname, format_type name)`.
const PG_TYPES: &[(i64, &str, &str)] = &[
	(16, "bool", "boolean"),
	(17, "bytea", "bytea"),
	(18, "char", "char"),
	(19, "name", "name"),
	(20, "int8", "bigint"),
	(21, "int2", "smallint"),
	(23, "int4", "integer"),
	(25, "text", "text"),
	(26, "oid", "oid"),
	(114, "json", "json"),
	(700, "float4", "real"),
	(701, "float8", "double precision"),
	(1042, "bpchar", "character"),
	(1043, "varchar", "character varying"),
	(1082, "date", "date"),
	(1083, "time", "time without time zone"),
	(1114, "timestamp", "timestamp without time zone"),
	(1184, "timestamptz", "timestamp with time zone"),
	(1186, "interval", "interval"),
	(1700, "numeric", "numeric"),
	(2950, "uuid", "uuid"),
	(3802, "jsonb", "jsonb"),
	(1000, "_bool", "boolean[]"),
	(1005, "_int2", "smallint[]"),
	(1007, "_int4", "integer[]"),
	(1016, "_int8", "bigint[]"),
	(1009, "_text", "text[]"),
	(1015, "_varchar", "character varying[]"),
	(1021, "_float4", "real[]"),
	(1022, "_float8", "double precision[]"),
	(2951, "_uuid", "uuid[]"),
	(3807, "_jsonb", "jsonb[]"),
	(crate::descriptor::VECTOR_OID as i64, "vector", "vector"),
	(crate::descriptor::VECTOR_ARRAY_OID as i64, "_vector", "vector[]"),
];

/// The largest oid of a built-in type `format_type` names.
const MAX_BUILTIN_TYPE_OID: i64 = 6200;

/// Each built-in type oid with the name `format_type` gives it without a modifier.
fn other_builtin_type_names() -> &'static [(i64, String)] {
	static NAMES: std::sync::OnceLock<Vec<(i64, String)>> = std::sync::OnceLock::new();
	NAMES.get_or_init(|| {
		(1..=MAX_BUILTIN_TYPE_OID)
			.filter_map(|oid| crate::reported_type::pg_format_type(oid, -1).map(|name| (oid, name)))
			.collect()
	})
}

/// The oid of the type `name` names: a type name, optionally schema-qualified and quoted, or one of
/// the SQL spellings of the integer types.
pub(crate) fn regtype_oid(name: &str) -> Option<i64> {
	let name = name.trim().replace('"', "");
	let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
	let name = match name {
		"int" | "integer" => "int4",
		"bigint" => "int8",
		"smallint" => "int2",
		other => other,
	};
	PG_TYPES
		.iter()
		.find(|(_, typname, format)| *typname == name || *format == name)
		.map(|(oid, ..)| *oid)
}

/// Lower a text literal cast to an array of two or more dimensions, `'{{1,2},{3,4}}'::int[][]`:
/// the literal is parsed as a nested array and each element is cast to the element type, so a
/// ragged literal is refused as malformed. A cast to one dimension, or of an operand that is not a
/// text literal, is `None` and lowers as it always has.
fn lower_multidim_array_cast(
	expr: &BoundExpr,
	target_type: &str,
) -> Result<Option<LogicalExpr>, PlanError> {
	let dimensions = target_type.matches("[]").count();
	if dimensions < 2 || !target_type.ends_with("[]") {
		return Ok(None);
	}
	let BoundExpr::Literal(BoundLiteral::String(text)) = expr else {
		return Ok(None);
	};
	let element = target_type.trim_end_matches("[]").to_lowercase();
	let kind = cast_kind(&element, SqlDialect::Postgres);
	let items = crate::array_literal::parse_array_literal(text).ok_or_else(|| {
		PlanError::InvalidTextRepresentation(format!("malformed array literal: \"{text}\""))
	})?;
	fn leaf(text: &str, kind: &Kind) -> LogicalExpr {
		let string = LogicalExpr::Literal(LogicalLiteral::String(Strand::from(text)));
		if matches!(kind, Kind::Any) {
			string
		} else {
			LogicalExpr::Prefix {
				op: PrefixOperator::Cast(kind.clone()),
				expr: Box::new(string),
			}
		}
	}
	fn nested(items: Vec<crate::array_literal::ArrayElement>, kind: &Kind) -> LogicalExpr {
		LogicalExpr::Array(
			items
				.into_iter()
				.map(|item| match item {
					crate::array_literal::ArrayElement::Null => {
						LogicalExpr::Literal(LogicalLiteral::Null)
					}
					crate::array_literal::ArrayElement::Text(text) => leaf(&text, kind),
					crate::array_literal::ArrayElement::Array(items) => nested(items, kind),
				})
				.collect(),
		)
	}
	Ok(Some(nested(items, &kind)))
}

/// Lower the array literal `text` cast to `regtype[]`: the array of the oids of the types it
/// names, as `x = ANY (...)` compares against a type oid. A name that is no known type is kept as
/// text.
fn lower_regtype_array_literal(text: &str) -> Result<LogicalExpr, PlanError> {
	let items = parse_text_array_literal(text).ok_or_else(|| {
		PlanError::InvalidTextRepresentation(format!("malformed array literal: \"{text}\""))
	})?;
	Ok(LogicalExpr::Array(
		items
			.into_iter()
			.map(|item| match item {
				None => LogicalExpr::Literal(LogicalLiteral::Null),
				Some(name) => match regtype_oid(&name) {
					Some(oid) => LogicalExpr::Literal(LogicalLiteral::Integer(oid)),
					None => LogicalExpr::Literal(LogicalLiteral::String(Strand::from(name))),
				},
			})
			.collect(),
	))
}

/// Lower `value::regtype` for a type named by text: a type name, optionally schema-qualified and
/// quoted (`bigint`, `int8`, `"pg_catalog"."int8"`), renders as the name `format_type` gives it.
/// A name that is no known type renders unchanged. The name is looked up in one object literal,
/// so `value` is evaluated twice, not once per known type.
fn lower_regtype(value: LogicalExpr) -> LogicalExpr {
	let mut names: Vec<(String, LogicalExpr)> = Vec::new();
	for (_, typname, name) in PG_TYPES {
		for key in [
			(*typname).to_string(),
			format!("\"pg_catalog\".\"{typname}\""),
			format!("pg_catalog.{typname}"),
			(*name).to_string(),
		] {
			if !names.iter().any(|(existing, _)| *existing == key) {
				names
					.push((key, LogicalExpr::Literal(LogicalLiteral::String(Strand::from(*name)))));
			}
		}
	}
	let known = LogicalExpr::Index {
		base: Box::new(LogicalExpr::Object(names)),
		index: Box::new(value.clone()),
	};
	// An oid names the type it numbers; any other value is shown unchanged.
	let branches = PG_TYPES
		.iter()
		.map(|(oid, _, name)| {
			(
				LogicalExpr::Binary {
					left: Box::new(LogicalExpr::Current),
					op: BinaryOperator::Equal,
					right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(*oid))),
				},
				LogicalExpr::Literal(LogicalLiteral::String(Strand::from(*name))),
			)
		})
		.collect();
	let by_oid = LogicalExpr::Optional {
		base: Box::new(value),
		then: Box::new(LogicalExpr::If(Box::new(LogicalIf {
			branches,
			otherwise: Some(LogicalExpr::Current),
		}))),
	};
	LogicalExpr::Binary {
		left: Box::new(known),
		op: BinaryOperator::NullCoalescing,
		right: Box::new(by_oid),
	}
}
