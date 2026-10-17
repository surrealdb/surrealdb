//! Lowering from BoundExpr to SurrealDB `LogicalExpr`.

use surrealdb_expr::expr::logical::{
	LogicalCall, LogicalExpr, LogicalField, LogicalIf, LogicalLiteral, LogicalOrder, OutputPath,
	RelationAlias,
};
use surrealdb_expr::expr::{BinaryOperator, Function, Kind, Param, PrefixOperator};
use surrealdb_strand::Strand;

use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::functions::FunctionRegistry;
use crate::ir::{
	BoundBinaryOp, BoundExpr, BoundLiteral, BoundOrder, BoundOrderDirection, BoundTarget,
	BoundUnaryOp,
};
use crate::lower::plan::lower_bound_subquery;
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
			if col.correlation_depth > 0 {
				let mut expr = LogicalExpr::Param(Param::from("parent".to_string()));
				for _ in 1..col.correlation_depth {
					expr = LogicalExpr::Access {
						base: Box::new(expr),
						field: Strand::from("parent"),
					};
				}
				Ok(LogicalExpr::Access {
					base: Box::new(expr),
					field: Strand::from(col.column.as_str()),
				})
			} else if col.column == "*" {
				Ok(LogicalExpr::Field(Strand::from("*")))
			} else if col.relation_index == 0 {
				// Base table in join or single-table query: field is directly on the row
				Ok(LogicalExpr::Field(Strand::from(col.column.as_str())))
			} else {
				// Joined table at index k >= 1: field is nested under _join{k-1}
				let alias = RelationAlias::new(col.relation_index - 1);
				Ok(alias.access(Strand::from(col.column.as_str())))
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
		} => lower_binary_op(left, *op, right, policy),
		BoundExpr::Unary {
			op,
			expr,
		} => {
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
						let group_call = LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("array::group".to_string()),
							args: vec![lowered_arg],
						}));
						return Ok(LogicalExpr::Call(Box::new(LogicalCall {
							receiver: Function::Normal("array::len".to_string()),
							args: vec![group_call],
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
					let l = lower_bound_expr(&args[0], policy)?;
					let r = lower_bound_expr(&args[1], policy)?;
					let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
					let match_call = LogicalExpr::Call(Box::new(LogicalCall {
						receiver: Function::Normal("string::matches".to_string()),
						args: vec![l, r],
					}));
					return Ok(LogicalExpr::If(Box::new(LogicalIf {
						branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
						otherwise: Some(match_call),
					})));
				}
				"current_database" | "database" | "schema" => {
					let db = policy.default_schema.unwrap_or("test");
					return Ok(LogicalExpr::Literal(LogicalLiteral::String(Strand::from(db))));
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
				"obj_description" | "col_description" | "shobj_description" | "pg_get_expr" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Null));
				}
				"pg_relation_size" | "pg_total_relation_size" => {
					return Ok(LogicalExpr::Literal(LogicalLiteral::Integer(0)));
				}
				"current_setting" => {
					let name_str = match &args[0] {
						BoundExpr::Literal(BoundLiteral::String(s)) => s.as_str(),
						_ => {
							return Err(PlanError::UnsupportedExpression(
								"current_setting requires a string literal argument".into(),
							));
						}
					};
					let val = match name_str.to_ascii_lowercase().as_str() {
						"timezone" | "time zone" => "UTC",
						"server_version" => "16.0-surrealdb",
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
						let key = match &chunk[0] {
							BoundExpr::Literal(BoundLiteral::String(s)) => s.clone(),
							other => {
								let k_low = lower_bound_expr(other, policy)?;
								format!("{k_low}")
							}
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
			let lower_args = if mapped_fn == "count"
				&& (args.is_empty()
					|| (args.len() == 1
						&& (matches!(&args[0], BoundExpr::Column(col) if col.column == "*")
							|| matches!(&args[0], BoundExpr::Literal(BoundLiteral::Integer(_))))))
			{
				Vec::new()
			} else if mapped_fn == "count" && args.len() == 1 {
				let inner = lower_bound_expr(&args[0], policy)?;
				let not_null = is_not_null_expr(inner);
				vec![LogicalExpr::If(Box::new(LogicalIf {
					branches: vec![(not_null, LogicalExpr::Literal(LogicalLiteral::Bool(true)))],
					otherwise: Some(LogicalExpr::Literal(LogicalLiteral::Null)),
				}))]
			} else {
				args.iter().map(|a| lower_bound_expr(a, policy)).collect::<Result<Vec<_>, _>>()?
			};
			Ok(LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal(mapped_fn.to_string()),
				args: lower_args,
			})))
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
		} => {
			let val = lower_bound_expr(expr, policy)?;
			let l = lower_bound_expr(low, policy)?;
			let h = lower_bound_expr(high, policy)?;
			let ge = LogicalExpr::Binary {
				left: Box::new(val.clone()),
				op: BinaryOperator::MoreThanEqual,
				right: Box::new(l),
			};
			let le = LogicalExpr::Binary {
				left: Box::new(val),
				op: BinaryOperator::LessThanEqual,
				right: Box::new(h),
			};
			let between = LogicalExpr::Binary {
				left: Box::new(ge),
				op: BinaryOperator::And,
				right: Box::new(le),
			};
			if *negated {
				Ok(LogicalExpr::Prefix {
					op: PrefixOperator::Not,
					expr: Box::new(between),
				})
			} else {
				Ok(between)
			}
		}
		BoundExpr::InList {
			expr,
			list,
			negated,
		} => {
			let val = lower_bound_expr(expr, policy)?;
			let items = list
				.iter()
				.map(|item| lower_bound_expr(item, policy))
				.collect::<Result<Vec<_>, _>>()?;
			let op = if *negated {
				BinaryOperator::NotInside
			} else {
				BinaryOperator::Inside
			};
			Ok(LogicalExpr::Binary {
				left: Box::new(val),
				op,
				right: Box::new(LogicalExpr::Array(items)),
			})
		}
		BoundExpr::IsNull {
			expr,
			negated,
		} => {
			let val = lower_bound_expr(expr, policy)?;
			let is_null = is_null_expr(val);
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
				Ok(lowered)
			} else {
				Ok(LogicalExpr::Prefix {
					op: PrefixOperator::Cast(kind),
					expr: Box::new(lowered),
				})
			}
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
			let val = lower_bound_expr(expr, policy)?;
			let sub_op = lower_bound_subquery(subquery, policy)?;
			let op = if *negated {
				BinaryOperator::NotInside
			} else {
				BinaryOperator::Inside
			};
			Ok(LogicalExpr::Binary {
				left: Box::new(val),
				op,
				right: Box::new(LogicalExpr::Subquery(Box::new(sub_op))),
			})
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

/// Lower binary operations preserving SQL 3VL, NULL propagation, and dialect rules.
fn lower_binary_op(
	left: &BoundExpr,
	op: BoundBinaryOp,
	right: &BoundExpr,
	policy: &DialectPolicy,
) -> Result<LogicalExpr, PlanError> {
	let l = lower_bound_expr(left, policy)?;
	let r = lower_bound_expr(right, policy)?;

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
			let match_call = match right {
				BoundExpr::Literal(BoundLiteral::String(pat)) => {
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
				}
				_ => LogicalExpr::Call(Box::new(LogicalCall {
					receiver: Function::Normal("string::matches".to_string()),
					args: vec![l, r],
				})),
			};
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(match_call),
			})))
		}
		BoundBinaryOp::ILike => {
			let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
			let match_call = match right {
				BoundExpr::Literal(BoundLiteral::String(pat)) => {
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
				}
				_ => LogicalExpr::Call(Box::new(LogicalCall {
					receiver: Function::Normal("string::matches".to_string()),
					args: vec![l, r],
				})),
			};
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(match_call),
			})))
		}
		BoundBinaryOp::Regexp => {
			let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
			let match_call = LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal("string::matches".to_string()),
				args: vec![l, r],
			}));
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(match_call),
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
		BoundBinaryOp::JsonExtract => {
			let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
			let extract_expr = match right {
				BoundExpr::Literal(BoundLiteral::String(field)) => LogicalExpr::Access {
					base: Box::new(l),
					field: Strand::from(field.as_str()),
				},
				_ => LogicalExpr::Index {
					base: Box::new(l),
					index: Box::new(r),
				},
			};
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(extract_expr),
			})))
		}
		BoundBinaryOp::JsonExtractText | BoundBinaryOp::JsonPathExtractText => {
			let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
			let extract_expr = match right {
				BoundExpr::Literal(BoundLiteral::String(field)) => LogicalExpr::Access {
					base: Box::new(l),
					field: Strand::from(field.as_str()),
				},
				_ => LogicalExpr::Index {
					base: Box::new(l),
					index: Box::new(r),
				},
			};
			let text_expr = LogicalExpr::Call(Box::new(LogicalCall {
				receiver: Function::Normal("type::string".to_string()),
				args: vec![extract_expr],
			}));
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(text_expr),
			})))
		}
		BoundBinaryOp::JsonContains => {
			let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(LogicalExpr::Binary {
					left: Box::new(l),
					op: BinaryOperator::Contain,
					right: Box::new(r),
				}),
			})))
		}
		_ => {
			let bin_op = match op {
				BoundBinaryOp::Add => BinaryOperator::Add,
				BoundBinaryOp::Subtract => BinaryOperator::Subtract,
				BoundBinaryOp::Multiply => BinaryOperator::Multiply,
				BoundBinaryOp::Remainder => BinaryOperator::Remainder,
				BoundBinaryOp::Equal => BinaryOperator::Equal,
				BoundBinaryOp::NotEqual => BinaryOperator::NotEqual,
				BoundBinaryOp::LessThan => BinaryOperator::LessThan,
				BoundBinaryOp::LessThanEqual => BinaryOperator::LessThanEqual,
				BoundBinaryOp::GreaterThan => BinaryOperator::MoreThan,
				BoundBinaryOp::GreaterThanEqual => BinaryOperator::MoreThanEqual,
				_ => unreachable!(),
			};
			let null_check = is_null_expr(l.clone()).or(is_null_expr(r.clone()));
			Ok(LogicalExpr::If(Box::new(LogicalIf {
				branches: vec![(null_check, LogicalExpr::Literal(LogicalLiteral::Null))],
				otherwise: Some(LogicalExpr::Binary {
					left: Box::new(l),
					op: bin_op,
					right: Box::new(r),
				}),
			})))
		}
	}
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

/// Lower a `BoundTarget` into a `LogicalField`.
pub fn lower_bound_target(
	target: &BoundTarget,
	policy: &DialectPolicy,
) -> Result<LogicalField, PlanError> {
	if target.is_wildcard {
		return Ok(LogicalField::All);
	}
	let expr = lower_bound_expr(&target.expr, policy)?;
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
			_ => None,
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
	let value = lower_bound_expr(&order.expr, policy)?;
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
			..
		} => {
			matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max")
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
