//! Lowering from BoundExpr to SurrealDB `LogicalExpr`.

use surrealdb_expr::expr::logical::{
	LogicalCall, LogicalExpr, LogicalField, LogicalIf, LogicalLiteral, LogicalOrder, OutputPath,
	RelationAlias,
};
use surrealdb_expr::expr::{BinaryOperator, Function, Kind, Param, PrefixOperator};
use surrealdb_strand::Strand;

use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::ir::{
	BoundBinaryOp, BoundExpr, BoundLiteral, BoundOrder, BoundOrderDirection, BoundTarget,
	BoundUnaryOp,
};
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
			if col.column == "*" {
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
			distinct: _,
		} => {
			let mapped_fn = match name.as_str() {
				"count" => "count",
				"sum" => "math::sum",
				"avg" => "math::mean",
				"min" => "math::min",
				"max" => "math::max",
				"abs" => "math::abs",
				"lower" => "string::lowercase",
				"upper" => "string::uppercase",
				"length" => "string::len",
				other => other,
			};
			let lower_args = if mapped_fn == "count"
				&& args.len() == 1
				&& matches!(&args[0], BoundExpr::Column(col) if col.column == "*")
			{
				Vec::new()
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
		BoundBinaryOp::Divide | BoundBinaryOp::IntDiv => {
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
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
			..
		} => false,
	}
}
