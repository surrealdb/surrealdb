//! Lowering from BoundExpr to SurrealDB `LogicalExpr`.

use surrealdb_expr::expr::logical::{
	LogicalCall, LogicalExpr, LogicalField, LogicalIf, LogicalLiteral, LogicalOrder, OutputPath,
	RelationAlias,
};
use surrealdb_expr::expr::{BinaryOperator, Function, Kind, Param, PrefixOperator};
use surrealdb_strand::Strand;

use crate::array_literal::{is_array_space, parse_text_array_literal};
use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::functions::FunctionRegistry;
use crate::ir::{
	BoundBinaryOp, BoundColumnRef, BoundExpr, BoundLiteral, BoundOrder, BoundOrderDirection,
	BoundTarget, BoundUnaryOp,
};
use crate::lower::id_key::{
	equal, id_column, is_key_or_link, key_of, key_operand, lower_id_key, record_of,
	record_of_operand, record_or_value,
};
use crate::lower::link::{
	LinkOperand, link_column, link_comparison_operand, link_operand, lower_stored, same_table_pair,
};
use crate::lower::plan::lower_bound_subquery;
use crate::lower::quote_ident::{lower_quote_ident, quote_ident_str};
use crate::lower::sql_like::like_pattern_regex;
use crate::semantics::sql_like_to_regex;

/// Lower a `BoundExpr` to a SurrealDB `LogicalExpr`.
pub fn lower_bound_expr(
	expr: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	match expr {
		BoundExpr::Literal(lit) => match lit {
			BoundLiteral::Integer(i) => Ok(LogicalExpr::Literal(LogicalLiteral::Integer(*i))),
			BoundLiteral::Float(f) => Ok(LogicalExpr::Literal(LogicalLiteral::Float(*f))),
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
						let group_call = LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("array::group".to_string()),
							args: vec![lowered_arg],
						}));
						return Ok(LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("math::sum".to_string()),
							args: vec![group_call],
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
						let group_call = LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("array::group".to_string()),
							args: vec![lowered_arg],
						}));
						return Ok(LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("math::mean".to_string()),
							args: vec![group_call],
						})));
					}
					"min" | "max" => {
						// MIN(DISTINCT x) is identical to MIN(x)
						// MAX(DISTINCT x) is identical to MAX(x)
					}
					"string_agg" | "group_concat" | "array_agg" | "json_agg" | "jsonb_agg" => {
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
						SqlDialect::MySql => "8.0.36-SurrealDB",
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
				"pg_get_userbyid" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from("root"))));
				}
				"format_type" => {
					let type_name = match args.first() {
						Some(BoundExpr::Literal(BoundLiteral::Integer(oid))) => match *oid {
							16 => "boolean",
							17 => "bytea",
							18 => "char",
							19 => "name",
							20 => "bigint",
							21 => "smallint",
							23 => "integer",
							25 => "text",
							26 => "oid",
							114 => "json",
							700 => "real",
							701 => "double precision",
							1042 => "character",
							1043 => "character varying",
							1082 => "date",
							1083 => "time without time zone",
							1114 => "timestamp without time zone",
							1184 => "timestamp with time zone",
							1186 => "interval",
							1700 => "numeric",
							2950 => "uuid",
							3802 => "jsonb",
							1000 => "boolean[]",
							1005 => "smallint[]",
							1007 => "integer[]",
							1016 => "bigint[]",
							1009 => "text[]",
							1015 => "character varying[]",
							1021 => "real[]",
							1022 => "double precision[]",
							2951 => "uuid[]",
							3807 => "jsonb[]",
							_ => "text",
						},
						_ => "text",
					};
					return Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(
						type_name,
					))));
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
				"obj_description" | "col_description" | "shobj_description" | "pg_get_expr" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
				}
				"pg_relation_size" | "pg_total_relation_size" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Integer(0)));
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
			if func_def.name == "group_concat" && args.len() > 1 {
				return Err(PlanError::UnsupportedExpression(
					"GROUP_CONCAT over several expressions".into(),
				));
			}
			let lower_args = if mapped_fn == "count"
				&& (args.is_empty()
					|| (args.len() == 1
						&& (matches!(&args[0], BoundExpr::Column(col) if col.column == "*")
							|| matches!(&args[0], BoundExpr::Literal(BoundLiteral::Integer(_))))))
			{
				Vec::new()
			} else if mapped_fn == "count" && args.len() == 1 {
				let inner = lower_stored(&args[0], policy)?;
				let not_null = is_not_null_expr(inner);
				vec![LogicalExpr::If(Box::new(LogicalIf {
					branches: vec![(not_null, LogicalExpr::Literal(LogicalLiteral::Bool(true)))],
					otherwise: Some(LogicalExpr::Literal(LogicalLiteral::Null)),
				}))]
			} else {
				args.iter().map(|a| lower_bound_expr(a, policy)).collect::<Result<Vec<_>, _>>()?
			};
			if func_def.is_scalar()
				&& mapped_fn.starts_with("math::")
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
			// all, and an array aggregate answers NULL over no row.
			let empty = match counted {
				Some(value) => Some(LogicalExpr::Binary {
					left: Box::new(LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("count".to_string()),
						args: vec![LogicalExpr::If(Box::new(LogicalIf {
							branches: vec![(
								is_not_null_expr(value),
								LogicalExpr::Literal(LogicalLiteral::Bool(true)),
							)],
							otherwise: Some(LogicalExpr::Literal(LogicalLiteral::Null)),
						}))],
					}))),
					op: BinaryOperator::Equal,
					right: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(0))),
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
					LogicalExpr::Binary {
						left: Box::new(lower_bound_expr(base, policy)?),
						op: BinaryOperator::Equal,
						right: Box::new(lower_bound_expr(cond, policy)?),
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
		} => Ok(sql_between(
			lower_bound_expr(expr, policy)?,
			lower_bound_expr(low, policy)?,
			lower_bound_expr(high, policy)?,
			*negated,
		)),
		BoundExpr::InList {
			expr,
			list,
			negated,
		} => {
			if let Some((col, table)) = id_column(expr)
				&& let Some((records, true)) = id_member_records(table, list, policy, true)?
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
			let val = lower_bound_expr(expr, policy)?;
			let items = list
				.iter()
				.map(|item| lower_bound_expr(item, policy))
				.collect::<Result<Vec<_>, _>>()?;
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
			let lowered = lower_bound_expr(expr, policy)?;
			let clean_type = target_type.to_lowercase();
			let base_type = clean_type.split('(').next().unwrap_or(&clean_type).trim();
			let kind = match base_type {
				"int" | "integer" | "int4" | "int8" | "int2" | "smallint" | "bigint" | "signed"
				| "unsigned" => Kind::Int,
				"float" | "float8" | "float4" | "double" | "real" | "double precision" => {
					Kind::Float
				}
				"numeric" | "decimal" | "dec" => Kind::Decimal,
				"text" | "varchar" | "char" | "character varying" | "bpchar" | "string" => {
					Kind::String
				}
				"bool" | "boolean" => Kind::Bool,
				"date" | "datetime" | "timestamp" | "timestamptz" => Kind::Datetime,
				_ => Kind::Any,
			};
			if matches!(kind, Kind::Any) {
				return Ok(lowered);
			}
			if matches!(lowered, LogicalExpr::Literal(LogicalLiteral::Null | LogicalLiteral::None))
			{
				return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
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
			let sub_op = lower_bound_subquery(subquery, policy)?;
			Ok(LogicalExpr::First(Box::new(LogicalExpr::Subquery(Box::new(sub_op)))))
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
		BoundExpr::WindowFunction {
			..
		} => {
			crate::observability::global_metrics().record_unsupported_feature("WINDOW");
			Err(PlanError::UnsupportedExpression(
				"window functions are not supported in streaming execution engine".into(),
			))
		}
	}
}

/// Lower a column reference to the stored field it names, with no key extraction.
pub(crate) fn lower_column_raw(col: &BoundColumnRef) -> LogicalExpr {
	if col.correlation_depth > 0 {
		let mut expr = LogicalExpr::Param(Param::from("parent".to_string()));
		for _ in 1..col.correlation_depth {
			expr = LogicalExpr::Access {
				base: Box::new(expr),
				field: Strand::from("parent"),
			};
		}
		LogicalExpr::Access {
			base: Box::new(expr),
			field: Strand::from(col.column.as_str()),
		}
	} else if col.column == "*" {
		LogicalExpr::Field(Strand::from("*"))
	} else if col.relation_index == 0 {
		// Base table in join or single-table query: field is directly on the row
		LogicalExpr::Field(Strand::from(col.column.as_str()))
	} else {
		// Joined table at index k >= 1: field is nested under _join{k-1}
		let alias = RelationAlias::new(col.relation_index - 1);
		alias.access(Strand::from(col.column.as_str()))
	}
}

/// The records of `table` that the members of an `IN` list of the `id` of `table` name, and whether
/// some member is a column or an expression, not a literal or a parameter. `None` when a member is
/// neither: a literal that is no key, or an expression that holds a subquery.
fn id_member_records(
	table: &str,
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
					record_or_value(table, lower_stored(member, policy)?)
				} else {
					record_of_operand(table, &key)
				},
			);
		} else if is_key_or_link(member) {
			has_value = true;
			let value = lower_stored(member, policy)?;
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
			Ok(Some(equal(lower_column_raw(col), record_of_operand(table, &key))))
		} else if is_key_or_link(other) {
			let record = record_of(table, lower_stored(other, policy)?);
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
				let LinkOperand::Record(record) = link_operand(table, member, policy)? else {
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
		let Some((records, _)) = id_member_records(table, list, policy, false)? else {
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
	Ok(match link_operand(table, other, policy)? {
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
		match link_comparison_operand(table, member, policy)? {
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
				let record = record_or_value(table, lower_stored(right, policy)?);
				return Ok((lower_column_raw(col), record));
			}
			(None, Some((col, table))) if is_key_or_link(left) => {
				let record = record_or_value(table, lower_stored(left, policy)?);
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
	Ok((lower_bound_expr(left, policy)?, lower_bound_expr(right, policy)?))
}

/// The two sides of a comparison of the link column `col` to `table` with `other`: the column (or
/// its key when `other` holds a subquery) and what `other` is compared as.
fn link_comparison(
	col: &BoundColumnRef,
	table: &str,
	other: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<(LogicalExpr, LogicalExpr), PlanError> {
	Ok(match link_comparison_operand(table, other, policy)? {
		LinkOperand::Record(other) | LinkOperand::Plain(other) => (lower_column_raw(col), other),
		LinkOperand::Subquery(other) => (key_of(lower_column_raw(col), true), other),
	})
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
			| BoundBinaryOp::JsonPathExtractText
			| BoundBinaryOp::JsonContains
	) {
		return lower_json_op(left, op, right, policy);
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
		| BoundBinaryOp::JsonPathExtractText
		| BoundBinaryOp::JsonContains
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
		return Err(PlanError::UnsupportedExpression("the @> containment operator".into()));
	}
	let Some(place) = lower_json_place(left, op, right, policy)? else {
		return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
	};
	Ok(if op == BoundBinaryOp::JsonExtract {
		absent_as_null(place)
	} else {
		json_text(place)
	})
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
	if op == BoundBinaryOp::JsonPathExtractText {
		let BoundExpr::Literal(BoundLiteral::String(path)) = right else {
			return Err(PlanError::UnsupportedExpression(
				"the path of #>> must be a text array literal such as '{a,b}'".into(),
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
				"the path of #>> has more than {MAX_JSON_PATH_STEPS} steps"
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
		BoundLiteral::Float(_) => Err(undefined("numeric")),
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
fn json_path_step(base: LogicalExpr, step: &str, two_sided_steps: &mut usize) -> LogicalExpr {
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
fn absent_as_null(value: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(value),
		op: BinaryOperator::NullCoalescing,
		right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
	}
}

/// The text `->>` and `#>>` give an extracted JSON value: NULL when the value is absent or null,
/// a string as itself, and anything else as its JSON text. `value` is evaluated once.
fn json_text(value: LogicalExpr) -> LogicalExpr {
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
		None => lower_bound_expr(&order.expr, policy)?,
	};
	let direction = match order.direction {
		BoundOrderDirection::Asc => true,
		BoundOrderDirection::Desc => false,
	};

	if direction && !order.nulls_first {
		// ASC NULLS LAST: false (non-null) before true (null)
		Ok(vec![
			LogicalOrder {
				value: is_null_expr(value.clone()),
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
				value: is_null_expr(value.clone()),
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
	let clean_name = name.to_ascii_lowercase();
	let val = match clean_name.as_str() {
		"version" => LogicalExpr::Literal(LogicalLiteral::String(Strand::from("8.0.36-SurrealDB"))),
		"version_comment" => {
			LogicalExpr::Literal(LogicalLiteral::String(Strand::from("SurrealDB")))
		}
		"version_compile_os" => {
			LogicalExpr::Literal(LogicalLiteral::String(Strand::from(std::env::consts::OS)))
		}
		"version_compile_machine" => {
			LogicalExpr::Literal(LogicalLiteral::String(Strand::from(std::env::consts::ARCH)))
		}
		"sql_mode" => LogicalExpr::Literal(LogicalLiteral::String(Strand::from(
			"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		))),
		"autocommit" => LogicalExpr::Literal(LogicalLiteral::Integer(1)),
		"character_set_client"
		| "character_set_connection"
		| "character_set_results"
		| "character_set_server"
		| "character_set_database" => {
			LogicalExpr::Literal(LogicalLiteral::String(Strand::from("utf8mb4")))
		}
		"collation_connection" | "collation_server" | "collation_database" => {
			LogicalExpr::Literal(LogicalLiteral::String(Strand::from("utf8mb4_0900_ai_ci")))
		}
		"time_zone" => LogicalExpr::Literal(LogicalLiteral::String(Strand::from("SYSTEM"))),
		"system_time_zone" => LogicalExpr::Literal(LogicalLiteral::String(Strand::from("UTC"))),
		"max_allowed_packet" => LogicalExpr::Literal(LogicalLiteral::Integer(67108864)),
		"interactive_timeout" | "wait_timeout" => {
			LogicalExpr::Literal(LogicalLiteral::Integer(28800))
		}
		"net_write_timeout" => LogicalExpr::Literal(LogicalLiteral::Integer(60)),
		"net_read_timeout" => LogicalExpr::Literal(LogicalLiteral::Integer(30)),
		"lower_case_table_names" => LogicalExpr::Literal(LogicalLiteral::Integer(0)),
		"auto_increment_increment" | "auto_increment_offset" => {
			LogicalExpr::Literal(LogicalLiteral::Integer(1))
		}
		"tx_isolation" | "transaction_isolation" => {
			LogicalExpr::Literal(LogicalLiteral::String(Strand::from("REPEATABLE-READ")))
		}
		"tx_read_only" | "transaction_read_only" => {
			LogicalExpr::Literal(LogicalLiteral::Integer(0))
		}
		"init_connect" => LogicalExpr::Literal(LogicalLiteral::String(Strand::from(""))),
		"query_cache_size" | "query_cache_type" | "performance_schema" => {
			LogicalExpr::Literal(LogicalLiteral::Integer(0))
		}
		"session_track_schema" => LogicalExpr::Literal(LogicalLiteral::Integer(1)),
		"session_track_system_variables" => {
			LogicalExpr::Literal(LogicalLiteral::String(Strand::from("")))
		}
		"session_track_state_change" => LogicalExpr::Literal(LogicalLiteral::Integer(0)),
		_ => LogicalExpr::Literal(LogicalLiteral::Null),
	};
	Ok(val)
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
