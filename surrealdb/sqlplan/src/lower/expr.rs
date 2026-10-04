//! Lowering from BoundExpr to SurrealDB `LogicalExpr`.

use surrealdb_expr::expr::logical::{
	LogicalCall, LogicalExpr, LogicalField, LogicalIf, LogicalLiteral, LogicalOrder, OutputPath,
	RelationAlias,
};
use surrealdb_expr::expr::{BinaryOperator, Function, Param, PrefixOperator};
use surrealdb_strand::Strand;

use crate::error::PlanError;
use crate::ir::{
	BoundBinaryOp, BoundExpr, BoundLiteral, BoundOrder, BoundOrderDirection, BoundTarget,
	BoundUnaryOp,
};

/// Lower a `BoundExpr` to a SurrealDB `LogicalExpr`.
pub fn lower_bound_expr(expr: &BoundExpr) -> Result<LogicalExpr, PlanError> {
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
			let base_expr = lower_bound_expr(base)?;
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
		} => {
			let l = lower_bound_expr(left)?;
			let r = lower_bound_expr(right)?;
			let bin_op = match op {
				BoundBinaryOp::Add => BinaryOperator::Add,
				BoundBinaryOp::Subtract => BinaryOperator::Subtract,
				BoundBinaryOp::Multiply => BinaryOperator::Multiply,
				BoundBinaryOp::Divide => BinaryOperator::Divide,
				BoundBinaryOp::Remainder => BinaryOperator::Remainder,
				BoundBinaryOp::Equal => BinaryOperator::Equal,
				BoundBinaryOp::NotEqual => BinaryOperator::NotEqual,
				BoundBinaryOp::LessThan => BinaryOperator::LessThan,
				BoundBinaryOp::LessThanEqual => BinaryOperator::LessThanEqual,
				BoundBinaryOp::GreaterThan => BinaryOperator::MoreThan,
				BoundBinaryOp::GreaterThanEqual => BinaryOperator::MoreThanEqual,
				BoundBinaryOp::And => BinaryOperator::And,
				BoundBinaryOp::Or => BinaryOperator::Or,
				BoundBinaryOp::Concat => BinaryOperator::Add,
				BoundBinaryOp::NullSafeEqual => BinaryOperator::ExactEqual,
			};
			Ok(LogicalExpr::Binary {
				left: Box::new(l),
				op: bin_op,
				right: Box::new(r),
			})
		}
		BoundExpr::Unary {
			op,
			expr,
		} => {
			let inner = lower_bound_expr(expr)?;
			match op {
				BoundUnaryOp::Not => Ok(LogicalExpr::Prefix {
					op: PrefixOperator::Not,
					expr: Box::new(inner),
				}),
				BoundUnaryOp::Positive => Ok(inner),
				BoundUnaryOp::Negate => Ok(LogicalExpr::Prefix {
					op: PrefixOperator::Negate,
					expr: Box::new(inner),
				}),
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
			let lower_args = args.iter().map(lower_bound_expr).collect::<Result<Vec<_>, _>>()?;
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
						left: Box::new(lower_bound_expr(base)?),
						op: BinaryOperator::Equal,
						right: Box::new(lower_bound_expr(cond)?),
					}
				} else {
					lower_bound_expr(cond)?
				};
				let value = lower_bound_expr(val)?;
				lower_branches.push((condition, value));
			}
			let otherwise = else_expr.as_ref().map(|e| lower_bound_expr(e)).transpose()?;
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
			let val = lower_bound_expr(expr)?;
			let l = lower_bound_expr(low)?;
			let h = lower_bound_expr(high)?;
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
			let val = lower_bound_expr(expr)?;
			let items = list.iter().map(lower_bound_expr).collect::<Result<Vec<_>, _>>()?;
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
			let val = lower_bound_expr(expr)?;
			let is_null = LogicalExpr::Binary {
				left: Box::new(val),
				op: BinaryOperator::Equal,
				right: Box::new(LogicalExpr::Literal(LogicalLiteral::Null)),
			};
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
			..
		} => lower_bound_expr(expr),
	}
}

/// Lower a `BoundTarget` into a `LogicalField`.
pub fn lower_bound_target(target: &BoundTarget) -> Result<LogicalField, PlanError> {
	if target.is_wildcard {
		return Ok(LogicalField::All);
	}
	let expr = lower_bound_expr(&target.expr)?;
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

/// Lower a `BoundOrder` into a `LogicalOrder`.
pub fn lower_bound_order(order: &BoundOrder) -> Result<LogicalOrder, PlanError> {
	let value = lower_bound_expr(&order.expr)?;
	let direction = match order.direction {
		BoundOrderDirection::Asc => true,
		BoundOrderDirection::Desc => false,
	};
	Ok(LogicalOrder {
		value,
		direction,
		collate: false,
		numeric: false,
	})
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
