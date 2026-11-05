//! SQL semantics layer: constant folding, 3VL logic, NULL propagation,
//! dialect-aware operators, division rules, and type coercion.

use regex::Regex;
use surrealdb_types::BigDecimal;

use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::ir::{
	BoundBinaryOp, BoundExpr, BoundLiteral, BoundSelect, BoundTableSource, BoundUnaryOp,
	BoundWindowFrameBound,
};

/// Fold all constant expressions in a `BoundSelect` statement according to the dialect policy.
pub fn fold_select(select: &mut BoundSelect, policy: &DialectPolicy) -> Result<(), PlanError> {
	// 1. Fold targets
	for target in &mut select.targets {
		target.expr = fold_expr(&target.expr, policy)?;
	}

	// 2. Fold WHERE clause
	if let Some(where_expr) = &mut select.where_clause {
		*where_expr = fold_expr(where_expr, policy)?;
	}

	// 3. Fold GROUP BY expressions
	for group_expr in &mut select.group_by {
		*group_expr = fold_expr(group_expr, policy)?;
	}

	// 4. Fold HAVING clause
	if let Some(having_expr) = &mut select.having {
		*having_expr = fold_expr(having_expr, policy)?;
	}

	// 5. Fold ORDER BY expressions
	for order in &mut select.order_by {
		order.expr = fold_expr(&order.expr, policy)?;
	}

	// 6. Fold LIMIT and OFFSET
	if let Some(limit) = &mut select.limit {
		*limit = fold_expr(limit, policy)?;
	}
	if let Some(offset) = &mut select.offset {
		*offset = fold_expr(offset, policy)?;
	}

	// 7. Fold FROM sources recursively if subqueries
	if let Some(from) = &mut select.from {
		fold_table_source(from, policy)?;
	}

	// 8. Fold set_op recursively if present
	if let Some(set_op) = &mut select.set_op {
		fold_select(&mut set_op.right, policy)?;
	}

	// 9. Fold CTE queries recursively if present
	if let Some(with) = &mut select.with {
		for cte in &mut with.ctes {
			fold_select(&mut cte.query, policy)?;
		}
	}

	// 10. Fold named WINDOW definitions
	for win in &mut select.windows {
		for p in &mut win.spec.partition_by {
			*p = fold_expr(p, policy)?;
		}
		for o in &mut win.spec.order_by {
			o.expr = fold_expr(&o.expr, policy)?;
		}
		if let Some(frame) = &mut win.spec.frame {
			if let BoundWindowFrameBound::Preceding(e) | BoundWindowFrameBound::Following(e) =
				&mut frame.start
			{
				**e = fold_expr(e, policy)?;
			}
			if let Some(BoundWindowFrameBound::Preceding(e) | BoundWindowFrameBound::Following(e)) =
				&mut frame.end
			{
				**e = fold_expr(e, policy)?;
			}
		}
	}

	Ok(())
}

fn fold_table_source(
	source: &mut BoundTableSource,
	policy: &DialectPolicy,
) -> Result<(), PlanError> {
	match source {
		BoundTableSource::Subquery {
			query,
			..
		} => fold_select(query, policy),
		BoundTableSource::Lateral {
			targets,
			..
		} => {
			for target in targets {
				target.expr = fold_expr(&target.expr, policy)?;
			}
			Ok(())
		}
		BoundTableSource::Join {
			left,
			right,
			condition,
			..
		} => {
			fold_table_source(left, policy)?;
			fold_table_source(right, policy)?;
			if let Some(crate::ir::BoundJoinCondition::On(cond)) = condition {
				*cond = fold_expr(cond, policy)?;
			}
			Ok(())
		}
		BoundTableSource::Table {
			..
		}
		| BoundTableSource::Values => Ok(()),
	}
}

/// Recursively fold and evaluate constant expressions in `BoundExpr`.
pub fn fold_expr(expr: &BoundExpr, policy: &DialectPolicy) -> Result<BoundExpr, PlanError> {
	match expr {
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
			..
		}
		| BoundExpr::UserVar(_)
		| BoundExpr::SystemVar {
			..
		} => Ok(expr.clone()),

		BoundExpr::Access {
			base,
			field,
		} => {
			let folded_base = fold_expr(base, policy)?;
			Ok(BoundExpr::Access {
				base: Box::new(folded_base),
				field: field.clone(),
			})
		}

		BoundExpr::Unary {
			op,
			expr: inner,
		} => {
			let folded_inner = fold_expr(inner, policy)?;
			if let BoundExpr::Literal(lit) = &folded_inner {
				let result = eval_unary_op(*op, lit, policy)?;
				Ok(BoundExpr::Literal(result))
			} else {
				Ok(BoundExpr::Unary {
					op: *op,
					expr: Box::new(folded_inner),
				})
			}
		}

		BoundExpr::Binary {
			left,
			op,
			right,
		} => {
			let folded_left = fold_expr(left, policy)?;
			let folded_right = fold_expr(right, policy)?;

			if let (BoundExpr::Literal(l), BoundExpr::Literal(r)) = (&folded_left, &folded_right)
				&& policy.dialect == SqlDialect::Postgres
			{
				if *op == BoundBinaryOp::Divide {
					numeric_quotient_in_range(l, r)?;
				} else if *op == BoundBinaryOp::Power {
					numeric_power_in_range(l, r)?;
				}
			}
			if let (BoundExpr::Literal(l), BoundExpr::Literal(r)) = (&folded_left, &folded_right)
				&& !leaves_numeric_to_engine(l, *op, r)
			{
				let result = eval_binary_op(l, *op, r, policy)?;
				Ok(BoundExpr::Literal(result))
			} else {
				// Short-circuit / simplify partially constant logical expressions
				if *op == BoundBinaryOp::And {
					if let BoundExpr::Literal(BoundLiteral::Boolean(false)) = folded_left {
						return Ok(BoundExpr::Literal(BoundLiteral::Boolean(false)));
					}
					if let BoundExpr::Literal(BoundLiteral::Boolean(false)) = folded_right {
						return Ok(BoundExpr::Literal(BoundLiteral::Boolean(false)));
					}
				} else if *op == BoundBinaryOp::Or {
					if let BoundExpr::Literal(BoundLiteral::Boolean(true)) = folded_left {
						return Ok(BoundExpr::Literal(BoundLiteral::Boolean(true)));
					}
					if let BoundExpr::Literal(BoundLiteral::Boolean(true)) = folded_right {
						return Ok(BoundExpr::Literal(BoundLiteral::Boolean(true)));
					}
				}

				Ok(BoundExpr::Binary {
					left: Box::new(folded_left),
					op: *op,
					right: Box::new(folded_right),
				})
			}
		}

		BoundExpr::FunctionCall {
			name,
			args,
			distinct,
		} => {
			let folded_args =
				args.iter().map(|a| fold_expr(a, policy)).collect::<Result<Vec<_>, _>>()?;
			Ok(BoundExpr::FunctionCall {
				name: name.clone(),
				args: folded_args,
				distinct: *distinct,
			})
		}

		BoundExpr::Case {
			operand,
			branches,
			else_expr,
		} => {
			let folded_operand =
				operand.as_ref().map(|op| fold_expr(op, policy)).transpose()?.map(Box::new);
			let mut folded_branches = Vec::with_capacity(branches.len());
			for (cond, val) in branches {
				folded_branches.push((fold_expr(cond, policy)?, fold_expr(val, policy)?));
			}
			let folded_else =
				else_expr.as_ref().map(|el| fold_expr(el, policy)).transpose()?.map(Box::new);
			Ok(BoundExpr::Case {
				operand: folded_operand,
				branches: folded_branches,
				else_expr: folded_else,
			})
		}

		BoundExpr::Between {
			expr: inner,
			low,
			high,
			negated,
		} => {
			let folded_inner = fold_expr(inner, policy)?;
			let folded_low = fold_expr(low, policy)?;
			let folded_high = fold_expr(high, policy)?;

			if let (BoundExpr::Literal(v), BoundExpr::Literal(l), BoundExpr::Literal(h)) =
				(&folded_inner, &folded_low, &folded_high)
			{
				let result = eval_between(v, l, h, *negated, policy)?;
				Ok(BoundExpr::Literal(result))
			} else {
				Ok(BoundExpr::Between {
					expr: Box::new(folded_inner),
					low: Box::new(folded_low),
					high: Box::new(folded_high),
					negated: *negated,
				})
			}
		}

		BoundExpr::InList {
			expr: inner,
			list,
			negated,
		} => {
			let folded_inner = fold_expr(inner, policy)?;
			let folded_list =
				list.iter().map(|item| fold_expr(item, policy)).collect::<Result<Vec<_>, _>>()?;

			let all_literals = folded_list.iter().all(|item| matches!(item, BoundExpr::Literal(_)));

			if let BoundExpr::Literal(v) = &folded_inner
				&& all_literals
			{
				let lits = folded_list
					.iter()
					.map(|item| {
						let BoundExpr::Literal(l) = item else {
							unreachable!()
						};
						l.clone()
					})
					.collect::<Vec<_>>();
				let result = eval_in_list(v, &lits, *negated, policy)?;
				Ok(BoundExpr::Literal(result))
			} else {
				Ok(BoundExpr::InList {
					expr: Box::new(folded_inner),
					list: folded_list,
					negated: *negated,
				})
			}
		}

		BoundExpr::IsNull {
			expr: inner,
			negated,
		} => {
			let folded_inner = fold_expr(inner, policy)?;
			if let BoundExpr::Literal(lit) = &folded_inner {
				let result = eval_is_null(lit, *negated);
				Ok(BoundExpr::Literal(result))
			} else {
				Ok(BoundExpr::IsNull {
					expr: Box::new(folded_inner),
					negated: *negated,
				})
			}
		}

		BoundExpr::Cast {
			expr: inner,
			target_type,
		} => {
			let folded_inner = fold_expr(inner, policy)?;
			// A blank-padded value cast to a type that is not blank-padded drops its trailing
			// spaces, so a folded `char(n)` or `bpchar` cast keeps that duty for the cast above it.
			let folded_inner = if let BoundExpr::Literal(BoundLiteral::String(text)) = &folded_inner
				&& policy.dialect == SqlDialect::Postgres
				&& matches!(
					inner.as_ref(),
					BoundExpr::Cast { target_type: inner_type, .. }
						if crate::lower::expr::is_blank_padded(inner_type)
				) && is_unpadded_text_type(target_type)
			{
				BoundExpr::Literal(BoundLiteral::String(text.trim_end_matches(' ').to_owned()))
			} else {
				folded_inner
			};
			// A cast to an array type is read when the expression is lowered, which knows the
			// element type, and so is a cast to `vector`, whose text form the lowering knows, and
			// a cast to a type with a family of its own, which the lowering reads with the
			// type's reader; there is no literal to fold any of them to.
			if let BoundExpr::Literal(lit) = &folded_inner
				&& !target_type.ends_with("[]")
				&& !is_temporal_type(target_type)
				&& !target_type.starts_with("vector")
				&& !target_type.starts_with("enum:")
				&& !is_exact_numeric_cast(lit, target_type)
				&& !(policy.dialect == SqlDialect::Postgres
					&& crate::lower::family_cast::is_family_target(target_type))
			{
				let result = eval_cast(lit, target_type, policy)?;
				Ok(BoundExpr::Literal(result))
			} else {
				Ok(BoundExpr::Cast {
					expr: Box::new(folded_inner),
					target_type: target_type.clone(),
				})
			}
		}
		BoundExpr::ScalarSubquery(sub) => {
			let mut sub = sub.clone();
			fold_select(&mut sub, policy)?;
			Ok(BoundExpr::ScalarSubquery(sub))
		}
		BoundExpr::Exists(sub) => {
			let mut sub = sub.clone();
			fold_select(&mut sub, policy)?;
			Ok(BoundExpr::Exists(sub))
		}
		BoundExpr::InSubquery {
			expr: inner,
			subquery,
			negated,
		} => {
			let folded_inner = fold_expr(inner, policy)?;
			let mut sub = subquery.clone();
			fold_select(&mut sub, policy)?;
			Ok(BoundExpr::InSubquery {
				expr: Box::new(folded_inner),
				subquery: sub,
				negated: *negated,
			})
		}
		BoundExpr::ArraySubquery(sub) => {
			let mut sub = sub.clone();
			fold_select(&mut sub, policy)?;
			Ok(BoundExpr::ArraySubquery(sub))
		}
		BoundExpr::WindowFunction {
			name,
			args,
			distinct,
			spec,
		} => {
			let folded_args =
				args.iter().map(|a| fold_expr(a, policy)).collect::<Result<Vec<_>, _>>()?;
			let folded_partition = spec
				.partition_by
				.iter()
				.map(|p| fold_expr(p, policy))
				.collect::<Result<Vec<_>, _>>()?;
			let mut folded_order = Vec::with_capacity(spec.order_by.len());
			for o in &spec.order_by {
				folded_order.push(crate::ir::BoundOrder {
					expr: fold_expr(&o.expr, policy)?,
					direction: o.direction,
					nulls_first: o.nulls_first,
				});
			}
			let folded_frame = if let Some(frame) = &spec.frame {
				let start = match &frame.start {
					BoundWindowFrameBound::Preceding(e) => {
						BoundWindowFrameBound::Preceding(Box::new(fold_expr(e, policy)?))
					}
					BoundWindowFrameBound::Following(e) => {
						BoundWindowFrameBound::Following(Box::new(fold_expr(e, policy)?))
					}
					BoundWindowFrameBound::CurrentRow
					| BoundWindowFrameBound::UnboundedPreceding
					| BoundWindowFrameBound::UnboundedFollowing => frame.start.clone(),
				};
				let end = match &frame.end {
					Some(BoundWindowFrameBound::Preceding(e)) => {
						Some(BoundWindowFrameBound::Preceding(Box::new(fold_expr(e, policy)?)))
					}
					Some(BoundWindowFrameBound::Following(e)) => {
						Some(BoundWindowFrameBound::Following(Box::new(fold_expr(e, policy)?)))
					}
					Some(
						BoundWindowFrameBound::CurrentRow
						| BoundWindowFrameBound::UnboundedPreceding
						| BoundWindowFrameBound::UnboundedFollowing,
					)
					| None => frame.end.clone(),
				};
				Some(crate::ir::BoundWindowFrame {
					units: frame.units,
					start,
					end,
					exclusion: frame.exclusion,
				})
			} else {
				None
			};
			Ok(BoundExpr::WindowFunction {
				name: name.clone(),
				args: folded_args,
				distinct: *distinct,
				spec: crate::ir::BoundWindowSpec {
					window_name: spec.window_name.clone(),
					partition_by: folded_partition,
					order_by: folded_order,
					frame: folded_frame,
				},
			})
		}
	}
}

/// Evaluate constant unary operation. Negating `i64::MIN` is `NumericValueOutOfRange`.
pub fn eval_unary_op(
	op: BoundUnaryOp,
	lit: &BoundLiteral,
	policy: &DialectPolicy,
) -> Result<BoundLiteral, PlanError> {
	match op {
		BoundUnaryOp::Not => match lit {
			BoundLiteral::Null => Ok(BoundLiteral::Null), // 3VL: NOT NULL is NULL
			BoundLiteral::Boolean(b) => Ok(BoundLiteral::Boolean(!b)),
			BoundLiteral::Integer(i) => {
				// In MySQL: NOT 0 = 1, NOT (nonzero) = 0
				Ok(BoundLiteral::Boolean(*i == 0))
			}
			BoundLiteral::Float(f) => Ok(BoundLiteral::Boolean(*f == 0.0)),
			BoundLiteral::Numeric(text) => Ok(BoundLiteral::Boolean(numeric_f64(text) == 0.0)),
			BoundLiteral::Unsigned(u) => Ok(BoundLiteral::Boolean(*u == 0)),
			_ => Ok(BoundLiteral::Null),
		},
		BoundUnaryOp::Positive => match lit {
			BoundLiteral::Null => Ok(BoundLiteral::Null),
			BoundLiteral::Integer(i) => Ok(BoundLiteral::Integer(*i)),
			BoundLiteral::Unsigned(u) => Ok(BoundLiteral::Unsigned(*u)),
			BoundLiteral::Float(f) => Ok(BoundLiteral::Float(*f)),
			BoundLiteral::Numeric(text) => Ok(BoundLiteral::Numeric(text.clone())),
			BoundLiteral::String(s) if policy.dialect == SqlDialect::MySql => {
				let (num, is_int) = mysql_parse_leading_number(s);
				if is_int {
					Ok(BoundLiteral::Integer(num as i64))
				} else {
					Ok(BoundLiteral::Float(num))
				}
			}
			BoundLiteral::String(_) | BoundLiteral::Boolean(_) => Ok(BoundLiteral::Null),
		},
		BoundUnaryOp::Negate => match lit {
			BoundLiteral::Null => Ok(BoundLiteral::Null),
			BoundLiteral::Integer(i) => checked_integer(i.checked_neg()),
			// A value above `i64::MAX` negates below the 64-bit range, which MySQL gives as a
			// double.
			BoundLiteral::Unsigned(u) => match i64::try_from(*u) {
				Ok(i) => checked_integer(i.checked_neg()),
				Err(_) => Ok(BoundLiteral::Float(-(*u as f64))),
			},
			BoundLiteral::Float(f) => Ok(BoundLiteral::Float(-*f)),
			BoundLiteral::Numeric(text) => {
				Ok(BoundLiteral::Numeric(match text.strip_prefix('-') {
					Some(magnitude) => magnitude.to_owned(),
					None => format!("-{text}"),
				}))
			}
			BoundLiteral::String(s) if policy.dialect == SqlDialect::MySql => {
				let (num, is_int) = mysql_parse_leading_number(s);
				if is_int {
					checked_integer((num as i64).checked_neg())
				} else {
					Ok(BoundLiteral::Float(-num))
				}
			}
			BoundLiteral::String(_) | BoundLiteral::Boolean(_) => Ok(BoundLiteral::Null),
		},
	}
}

/// Evaluate constant binary operation with full 3VL, division rules, and NULL propagation.
pub fn eval_binary_op(
	left: &BoundLiteral,
	op: BoundBinaryOp,
	right: &BoundLiteral,
	policy: &DialectPolicy,
) -> Result<BoundLiteral, PlanError> {
	// A distance is NULL over a NULL; no other constant is a vector.
	if let BoundBinaryOp::VectorDistance(_) = op {
		return match (left, right) {
			(BoundLiteral::Null, _) | (_, BoundLiteral::Null) => Ok(BoundLiteral::Null),
			_ => Err(PlanError::UndefinedOperator(
				"a vector distance over operands that are not vectors".to_string(),
			)),
		};
	}

	// 1. NullSafeEqual (`<=>`, `IS NOT DISTINCT FROM`)
	if op == BoundBinaryOp::NullSafeEqual {
		return match (left, right) {
			(BoundLiteral::Null, BoundLiteral::Null) => Ok(BoundLiteral::Boolean(true)),
			(BoundLiteral::Null, _) | (_, BoundLiteral::Null) => Ok(BoundLiteral::Boolean(false)),
			(l, r) => {
				let eq = literals_equal(l, r, policy)?;
				Ok(BoundLiteral::Boolean(eq))
			}
		};
	}

	// 2. DistinctFrom (`IS DISTINCT FROM`)
	if op == BoundBinaryOp::DistinctFrom {
		return match (left, right) {
			(BoundLiteral::Null, BoundLiteral::Null) => Ok(BoundLiteral::Boolean(false)),
			(BoundLiteral::Null, _) | (_, BoundLiteral::Null) => Ok(BoundLiteral::Boolean(true)),
			(l, r) => {
				let eq = literals_equal(l, r, policy)?;
				Ok(BoundLiteral::Boolean(!eq))
			}
		};
	}

	// 3. 3VL Logical AND
	if op == BoundBinaryOp::And {
		let l_bool = literal_to_bool(left);
		let r_bool = literal_to_bool(right);

		// If either is definitively false, the result is false (even if the other is NULL)
		if l_bool == Some(false) || r_bool == Some(false) {
			return Ok(BoundLiteral::Boolean(false));
		}
		// If both are true, result is true
		if l_bool == Some(true) && r_bool == Some(true) {
			return Ok(BoundLiteral::Boolean(true));
		}
		// Otherwise (true AND NULL, NULL AND true, NULL AND NULL), result is NULL
		return Ok(BoundLiteral::Null);
	}

	// 4. 3VL Logical OR
	if op == BoundBinaryOp::Or {
		let l_bool = literal_to_bool(left);
		let r_bool = literal_to_bool(right);

		// If either is definitively true, the result is true (even if the other is NULL)
		if l_bool == Some(true) || r_bool == Some(true) {
			return Ok(BoundLiteral::Boolean(true));
		}
		// If both are false, result is false
		if l_bool == Some(false) && r_bool == Some(false) {
			return Ok(BoundLiteral::Boolean(false));
		}
		// Otherwise (false OR NULL, NULL OR false, NULL OR NULL), result is NULL
		return Ok(BoundLiteral::Null);
	}

	// 5. NULL Propagation: for all other operations, if either operand is NULL, result is NULL
	if matches!(left, BoundLiteral::Null) || matches!(right, BoundLiteral::Null) {
		return Ok(BoundLiteral::Null);
	}

	// 6. String concatenation (`||`)
	if op == BoundBinaryOp::Concat {
		let l_str = literal_to_string(left);
		let r_str = literal_to_string(right);
		return Ok(BoundLiteral::String(format!("{l_str}{r_str}")));
	}

	// 7. Pattern matching (LIKE, ILIKE, REGEXP)
	if matches!(
		op,
		BoundBinaryOp::Like
			| BoundBinaryOp::ILike
			| BoundBinaryOp::Regexp
			| BoundBinaryOp::RegexpCi
	) {
		let text = literal_to_string(left);
		let pattern = literal_to_string(right);

		let matched = if op == BoundBinaryOp::Like {
			let case_insensitive = policy.dialect == SqlDialect::MySql;
			like_matches(&sql_like_to_regex(&pattern, case_insensitive), &text)?
		} else if op == BoundBinaryOp::ILike {
			like_matches(&sql_like_to_regex(&pattern, true), &text)?
		} else {
			let pattern = if op == BoundBinaryOp::RegexpCi {
				format!("(?i){pattern}")
			} else {
				pattern
			};
			Regex::new(&pattern)
				.map(|r| r.is_match(&text))
				.map_err(|e| PlanError::Custom(format!("invalid regular expression: {e}")))?
		};
		return Ok(BoundLiteral::Boolean(matched));
	}

	// 8. Comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`)
	if matches!(
		op,
		BoundBinaryOp::Equal
			| BoundBinaryOp::NotEqual
			| BoundBinaryOp::LessThan
			| BoundBinaryOp::LessThanEqual
			| BoundBinaryOp::GreaterThan
			| BoundBinaryOp::GreaterThanEqual
	) {
		return eval_comparison(left, op, right, policy);
	}

	// 9. Power (`^`)
	if op == BoundBinaryOp::Power {
		return eval_power(left, right);
	}

	// 10. Arithmetic operations (`+`, `-`, `*`, `/`, `%`, `DIV`)
	eval_arithmetic(left, op, right, policy)
}

/// Evaluate `left ^ right` on constants as the lowering does at run time: on double precision,
/// and NULL for a result that is not a finite number (a negative base to a fractional power, zero
/// to a negative power, overflow). A numeric operand against a float is first converted to a
/// double, which is refused when out of range.
fn eval_power(left: &BoundLiteral, right: &BoundLiteral) -> Result<BoundLiteral, PlanError> {
	float_coercion_in_range(left, right)?;
	let operand = |lit: &BoundLiteral| match lit {
		BoundLiteral::Integer(i) => Ok(*i as f64),
		BoundLiteral::Unsigned(u) => Ok(*u as f64),
		BoundLiteral::Float(f) => Ok(*f),
		BoundLiteral::Numeric(text) => Ok(numeric_f64(text)),
		BoundLiteral::String(s) => s
			.parse::<f64>()
			.map_err(|_| PlanError::InvalidTextRepresentation(format!("numeric: \"{s}\""))),
		other @ (BoundLiteral::Boolean(_) | BoundLiteral::Null) => {
			Err(PlanError::unsupported(format!("the ^ operator on {other:?}")))
		}
	};
	let power = operand(left)?.powf(operand(right)?);
	Ok(if power.is_finite() {
		BoundLiteral::Float(power)
	} else {
		BoundLiteral::Null
	})
}

/// Comparison evaluation between non-null literals.
fn eval_comparison(
	left: &BoundLiteral,
	op: BoundBinaryOp,
	right: &BoundLiteral,
	policy: &DialectPolicy,
) -> Result<BoundLiteral, PlanError> {
	float_coercion_in_range(left, right)?;
	let ord = compare_literals(left, right, policy);
	let result = match op {
		BoundBinaryOp::Equal => ord == Some(std::cmp::Ordering::Equal),
		BoundBinaryOp::NotEqual => ord != Some(std::cmp::Ordering::Equal),
		BoundBinaryOp::LessThan => ord == Some(std::cmp::Ordering::Less),
		BoundBinaryOp::LessThanEqual => {
			ord == Some(std::cmp::Ordering::Less) || ord == Some(std::cmp::Ordering::Equal)
		}
		BoundBinaryOp::GreaterThan => ord == Some(std::cmp::Ordering::Greater),
		BoundBinaryOp::GreaterThanEqual => {
			ord == Some(std::cmp::Ordering::Greater) || ord == Some(std::cmp::Ordering::Equal)
		}
		BoundBinaryOp::Add
		| BoundBinaryOp::Subtract
		| BoundBinaryOp::Multiply
		| BoundBinaryOp::Divide
		| BoundBinaryOp::Remainder
		| BoundBinaryOp::Power
		| BoundBinaryOp::And
		| BoundBinaryOp::Or
		| BoundBinaryOp::Concat
		| BoundBinaryOp::NullSafeEqual
		| BoundBinaryOp::DistinctFrom
		| BoundBinaryOp::Like
		| BoundBinaryOp::ILike
		| BoundBinaryOp::Regexp
		| BoundBinaryOp::RegexpCi
		| BoundBinaryOp::IntDiv
		| BoundBinaryOp::JsonExtract
		| BoundBinaryOp::JsonExtractText
		| BoundBinaryOp::JsonPathExtract
		| BoundBinaryOp::JsonPathExtractText
		| BoundBinaryOp::JsonContains
		| BoundBinaryOp::VectorDistance(_) => unreachable!(),
	};
	Ok(BoundLiteral::Boolean(result))
}

fn compare_literals(
	left: &BoundLiteral,
	right: &BoundLiteral,
	policy: &DialectPolicy,
) -> Option<std::cmp::Ordering> {
	if matches!(left, BoundLiteral::Numeric(_)) || matches!(right, BoundLiteral::Numeric(_)) {
		return compare_numeric(left, right, policy);
	}
	match (left, right) {
		(BoundLiteral::String(l), BoundLiteral::String(r)) => {
			if policy.dialect == SqlDialect::MySql {
				Some(l.to_lowercase().cmp(&r.to_lowercase()))
			} else {
				Some(l.cmp(r))
			}
		}
		(BoundLiteral::Boolean(l), BoundLiteral::Boolean(r)) => Some(l.cmp(r)),
		(BoundLiteral::Integer(l), BoundLiteral::Integer(r)) => Some(l.cmp(r)),
		(BoundLiteral::Float(l), BoundLiteral::Float(r)) => l.partial_cmp(r),
		(BoundLiteral::Integer(l), BoundLiteral::Float(r)) => (*l as f64).partial_cmp(r),
		(BoundLiteral::Float(l), BoundLiteral::Integer(r)) => l.partial_cmp(&(*r as f64)),
		// Integers compare exactly, including an unsigned value above `i64::MAX`.
		(
			l @ (BoundLiteral::Integer(_) | BoundLiteral::Unsigned(_)),
			r @ (BoundLiteral::Integer(_) | BoundLiteral::Unsigned(_)),
		) => Some(exact_integer(l)?.cmp(&exact_integer(r)?)),
		(BoundLiteral::Unsigned(l), BoundLiteral::Float(r)) => (*l as f64).partial_cmp(r),
		(BoundLiteral::Float(l), BoundLiteral::Unsigned(r)) => l.partial_cmp(&(*r as f64)),
		(BoundLiteral::String(s), BoundLiteral::Unsigned(u))
			if policy.dialect == SqlDialect::MySql =>
		{
			let (num, _) = mysql_parse_leading_number(s);
			num.partial_cmp(&(*u as f64))
		}
		(BoundLiteral::Unsigned(u), BoundLiteral::String(s))
			if policy.dialect == SqlDialect::MySql =>
		{
			let (num, _) = mysql_parse_leading_number(s);
			(*u as f64).partial_cmp(&num)
		}
		// In MySQL, string vs number coerces string to number
		(BoundLiteral::String(s), BoundLiteral::Integer(i))
			if policy.dialect == SqlDialect::MySql =>
		{
			let (num, is_int) = mysql_parse_leading_number(s);
			if is_int {
				Some((num as i64).cmp(i))
			} else {
				num.partial_cmp(&(*i as f64))
			}
		}
		(BoundLiteral::Integer(i), BoundLiteral::String(s))
			if policy.dialect == SqlDialect::MySql =>
		{
			let (num, is_int) = mysql_parse_leading_number(s);
			if is_int {
				Some(i.cmp(&(num as i64)))
			} else {
				(*i as f64).partial_cmp(&num)
			}
		}
		(BoundLiteral::String(s), BoundLiteral::Float(f))
			if policy.dialect == SqlDialect::MySql =>
		{
			let (num, _) = mysql_parse_leading_number(s);
			num.partial_cmp(f)
		}
		(BoundLiteral::Float(f), BoundLiteral::String(s))
			if policy.dialect == SqlDialect::MySql =>
		{
			let (num, _) = mysql_parse_leading_number(s);
			f.partial_cmp(&num)
		}
		_ => None,
	}
}

fn literals_equal(
	left: &BoundLiteral,
	right: &BoundLiteral,
	policy: &DialectPolicy,
) -> Result<bool, PlanError> {
	float_coercion_in_range(left, right)?;
	Ok(compare_literals(left, right, policy) == Some(std::cmp::Ordering::Equal))
}

/// A numeric operand of constant arithmetic. An integer is kept exact so that 64-bit arithmetic
/// can be checked.
#[derive(Clone, Copy)]
enum Number {
	Int(i64),
	Float(f64),
}

impl Number {
	fn as_f64(self) -> f64 {
		match self {
			Self::Int(i) => i as f64,
			Self::Float(f) => f,
		}
	}

	/// The value as a 64-bit integer when it is an integer or a float with no fraction that lies in
	/// the 64-bit range. NaN and the infinities have no integer value.
	fn as_exact_integer(self) -> Option<i64> {
		match self {
			Self::Int(i) => Some(i),
			// The doubles that are 64-bit integers run from -2^63 up to, but not including, 2^63.
			Self::Float(f)
				if f.fract() == 0.0 && f >= i64::MIN as f64 && f < -(i64::MIN as f64) =>
			{
				Some(f as i64)
			}
			Self::Float(_) => None,
		}
	}
}

/// The exact value of an integer literal. An unsigned literal can exceed `i64::MAX`.
fn exact_integer(lit: &BoundLiteral) -> Option<i128> {
	match lit {
		BoundLiteral::Integer(i) => Some(i128::from(*i)),
		BoundLiteral::Unsigned(u) => Some(i128::from(*u)),
		BoundLiteral::Float(_)
		| BoundLiteral::Numeric(_)
		| BoundLiteral::String(_)
		| BoundLiteral::Boolean(_)
		| BoundLiteral::Null => None,
	}
}

/// Integer arithmetic where an operand is unsigned, as MySQL does: the result is unsigned, so a
/// negative result or one above `u64::MAX` is `NumericValueOutOfRange`. A zero divisor is NULL.
/// `None` for an operator that is not integer arithmetic, which keeps its double-precision rule.
fn unsigned_arithmetic(
	a: i128,
	op: BoundBinaryOp,
	b: i128,
) -> Option<Result<BoundLiteral, PlanError>> {
	let exact = match op {
		BoundBinaryOp::Add => a + b,
		BoundBinaryOp::Subtract => a - b,
		BoundBinaryOp::Multiply => match a.checked_mul(b) {
			Some(product) => product,
			None => return Some(Err(PlanError::NumericValueOutOfRange)),
		},
		BoundBinaryOp::IntDiv | BoundBinaryOp::Remainder if b == 0 => {
			return Some(Ok(BoundLiteral::Null));
		}
		BoundBinaryOp::IntDiv => a / b,
		BoundBinaryOp::Remainder => a % b,
		BoundBinaryOp::Divide
		| BoundBinaryOp::Equal
		| BoundBinaryOp::NotEqual
		| BoundBinaryOp::LessThan
		| BoundBinaryOp::LessThanEqual
		| BoundBinaryOp::GreaterThan
		| BoundBinaryOp::GreaterThanEqual
		| BoundBinaryOp::And
		| BoundBinaryOp::Or
		| BoundBinaryOp::Concat
		| BoundBinaryOp::NullSafeEqual
		| BoundBinaryOp::DistinctFrom
		| BoundBinaryOp::Like
		| BoundBinaryOp::ILike
		| BoundBinaryOp::Regexp
		| BoundBinaryOp::RegexpCi
		| BoundBinaryOp::Power
		| BoundBinaryOp::JsonExtract
		| BoundBinaryOp::JsonExtractText
		| BoundBinaryOp::JsonPathExtract
		| BoundBinaryOp::JsonPathExtractText
		| BoundBinaryOp::JsonContains
		| BoundBinaryOp::VectorDistance(_) => return None,
	};
	Some(match u64::try_from(exact) {
		Ok(value) => Ok(BoundLiteral::Unsigned(value)),
		Err(_) => Err(PlanError::NumericValueOutOfRange),
	})
}

/// The integer result of a checked 64-bit operation, or the out of range error when it overflowed.
fn checked_integer(result: Option<i64>) -> Result<BoundLiteral, PlanError> {
	result.map(BoundLiteral::Integer).ok_or(PlanError::NumericValueOutOfRange)
}

/// Arithmetic evaluation with dialect division rules and string-to-number coercion.
///
/// Integer arithmetic is checked: a result beyond 64 bits is `NumericValueOutOfRange`, as
/// PostgreSQL's `bigint out of range` and MySQL's `BIGINT value is out of range`. `i64::MIN % -1`
/// is 0 in both dialects. `DIV` divides exactly in 64 bits when both operands have an integer value
/// that fits, a float such as `1.0` included, and otherwise divides in floating point, then
/// truncates.
fn eval_arithmetic(
	left: &BoundLiteral,
	op: BoundBinaryOp,
	right: &BoundLiteral,
	policy: &DialectPolicy,
) -> Result<BoundLiteral, PlanError> {
	if policy.dialect == SqlDialect::MySql
		&& (matches!(left, BoundLiteral::Unsigned(_)) || matches!(right, BoundLiteral::Unsigned(_)))
		&& let (Some(a), Some(b)) = (exact_integer(left), exact_integer(right))
		&& let Some(result) = unsigned_arithmetic(a, op, b)
	{
		return result;
	}
	if let Some(result) = exact_numeric_arithmetic(left, op, right, policy.dialect) {
		return result;
	}
	let l = to_number(left, policy)?;
	let r = to_number(right, policy)?;

	match op {
		BoundBinaryOp::Add => match (l, r) {
			(Number::Int(a), Number::Int(b)) => checked_integer(a.checked_add(b)),
			_ => Ok(BoundLiteral::Float(l.as_f64() + r.as_f64())),
		},
		BoundBinaryOp::Subtract => match (l, r) {
			(Number::Int(a), Number::Int(b)) => checked_integer(a.checked_sub(b)),
			_ => Ok(BoundLiteral::Float(l.as_f64() - r.as_f64())),
		},
		BoundBinaryOp::Multiply => match (l, r) {
			(Number::Int(a), Number::Int(b)) => checked_integer(a.checked_mul(b)),
			_ => Ok(BoundLiteral::Float(l.as_f64() * r.as_f64())),
		},
		BoundBinaryOp::Divide => {
			// Check division by zero
			if r.as_f64() == 0.0 {
				if policy.dialect == SqlDialect::Postgres {
					return Err(PlanError::DivisionByZero);
				} else {
					return Ok(BoundLiteral::Null);
				}
			}

			match (policy.dialect, l, r) {
				// In PostgreSQL: integer / integer produces integer division (1 / 2 = 0)
				(SqlDialect::Postgres, Number::Int(a), Number::Int(b)) => {
					checked_integer(a.checked_div(b))
				}
				// In MySQL: `/` is always float division (1 / 2 = 0.5)
				_ => Ok(BoundLiteral::Float(l.as_f64() / r.as_f64())),
			}
		}
		BoundBinaryOp::IntDiv => {
			// MySQL integer division (`DIV`)
			if r.as_f64() == 0.0 {
				return Ok(BoundLiteral::Null);
			}
			match (l.as_exact_integer(), r.as_exact_integer()) {
				// The divisor is not zero here, so `None` is `i64::MIN DIV -1`.
				(Some(a), Some(b)) => checked_integer(a.checked_div(b)),
				_ => match Number::Float((l.as_f64() / r.as_f64()).trunc()).as_exact_integer() {
					Some(quotient) => Ok(BoundLiteral::Integer(quotient)),
					None => Err(PlanError::NumericValueOutOfRange),
				},
			}
		}
		BoundBinaryOp::Remainder => {
			if r.as_f64() == 0.0 {
				if policy.dialect == SqlDialect::Postgres {
					return Err(PlanError::DivisionByZero);
				} else {
					return Ok(BoundLiteral::Null);
				}
			}
			match (l, r) {
				// `wrapping_rem` is 0 for `i64::MIN % -1`; the divisor is not zero here.
				(Number::Int(a), Number::Int(b)) => Ok(BoundLiteral::Integer(a.wrapping_rem(b))),
				_ => Ok(BoundLiteral::Float(l.as_f64() % r.as_f64())),
			}
		}
		BoundBinaryOp::Equal
		| BoundBinaryOp::NotEqual
		| BoundBinaryOp::LessThan
		| BoundBinaryOp::LessThanEqual
		| BoundBinaryOp::GreaterThan
		| BoundBinaryOp::GreaterThanEqual
		| BoundBinaryOp::Power
		| BoundBinaryOp::And
		| BoundBinaryOp::Or
		| BoundBinaryOp::Concat
		| BoundBinaryOp::NullSafeEqual
		| BoundBinaryOp::DistinctFrom
		| BoundBinaryOp::Like
		| BoundBinaryOp::ILike
		| BoundBinaryOp::Regexp
		| BoundBinaryOp::RegexpCi
		| BoundBinaryOp::JsonExtract
		| BoundBinaryOp::JsonExtractText
		| BoundBinaryOp::JsonPathExtract
		| BoundBinaryOp::JsonPathExtractText
		| BoundBinaryOp::JsonContains
		| BoundBinaryOp::VectorDistance(_) => {
			Err(PlanError::unsupported(format!("Arithmetic operation {op:?}")))
		}
	}
}

/// Convert a literal to a number.
fn to_number(lit: &BoundLiteral, policy: &DialectPolicy) -> Result<Number, PlanError> {
	match lit {
		BoundLiteral::Integer(i) => Ok(Number::Int(*i)),
		// An unsigned value above `i64::MAX` is a double here. Exact unsigned arithmetic is handled
		// before this conversion.
		BoundLiteral::Unsigned(u) => Ok(Number::Float(*u as f64)),
		BoundLiteral::Float(f) => Ok(Number::Float(*f)),
		BoundLiteral::Numeric(text) => Ok(Number::Float(numeric_as_float(text)?)),
		BoundLiteral::String(s) if policy.dialect == SqlDialect::MySql => {
			let (num, is_int) = mysql_parse_leading_number(s);
			Ok(if is_int {
				Number::Int(num as i64)
			} else {
				Number::Float(num)
			})
		}
		BoundLiteral::String(s) => {
			if let Ok(i) = s.parse::<i64>() {
				Ok(Number::Int(i))
			} else if let Ok(f) = s.parse::<f64>() {
				Ok(Number::Float(f))
			} else {
				Err(PlanError::InvalidTextRepresentation(format!("numeric: \"{s}\"")))
			}
		}
		BoundLiteral::Boolean(b) => Ok(Number::Int(i64::from(*b))),
		BoundLiteral::Null => Err(PlanError::Custom("cannot convert null to number".into())),
	}
}

/// Evaluate `IS NULL` / `IS NOT NULL`.
pub fn eval_is_null(lit: &BoundLiteral, negated: bool) -> BoundLiteral {
	let is_null = matches!(lit, BoundLiteral::Null);
	if negated {
		BoundLiteral::Boolean(!is_null)
	} else {
		BoundLiteral::Boolean(is_null)
	}
}

/// Evaluate `BETWEEN` expression with 3VL.
pub fn eval_between(
	expr: &BoundLiteral,
	low: &BoundLiteral,
	high: &BoundLiteral,
	negated: bool,
	policy: &DialectPolicy,
) -> Result<BoundLiteral, PlanError> {
	// If any operand is NULL, result is NULL (unless negated)
	if matches!(expr, BoundLiteral::Null)
		|| matches!(low, BoundLiteral::Null)
		|| matches!(high, BoundLiteral::Null)
	{
		return Ok(BoundLiteral::Null);
	}

	float_coercion_in_range(expr, low)?;
	float_coercion_in_range(expr, high)?;
	let gte = compare_literals(expr, low, policy);
	let lte = compare_literals(expr, high, policy);

	let in_range = matches!(
		(gte, lte),
		(
			Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
			Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal),
		)
	);

	let res = if negated {
		!in_range
	} else {
		in_range
	};
	Ok(BoundLiteral::Boolean(res))
}

/// Evaluate `IN` list expression with 3VL.
pub fn eval_in_list(
	expr: &BoundLiteral,
	list: &[BoundLiteral],
	negated: bool,
	policy: &DialectPolicy,
) -> Result<BoundLiteral, PlanError> {
	if matches!(expr, BoundLiteral::Null) {
		return Ok(BoundLiteral::Null);
	}

	let mut has_null = false;
	let mut matched = false;

	for item in list {
		if matches!(item, BoundLiteral::Null) {
			has_null = true;
			continue;
		}
		if literals_equal(expr, item, policy)? {
			matched = true;
			break;
		}
	}

	if matched {
		Ok(BoundLiteral::Boolean(!negated))
	} else if has_null {
		// In SQL: `x IN (1, 2, NULL)` where x is not in {1, 2} evaluates to NULL!
		Ok(BoundLiteral::Null)
	} else {
		Ok(BoundLiteral::Boolean(negated))
	}
}

/// Whether a cast to the type yields a character string that is not blank-padded: `text`,
/// `varchar(n)` or `name`.
fn is_unpadded_text_type(target_type: &str) -> bool {
	matches!(
		crate::descriptor::SqlType::from_name(
			target_type.split('(').next().unwrap_or_default().trim()
		),
		crate::descriptor::SqlType::Text
			| crate::descriptor::SqlType::Varchar
			| crate::descriptor::SqlType::Name
	)
}

/// Evaluate type cast.
/// Whether a cast to the type yields a date, a time, a timestamp or an interval, which no literal
/// is: the cast is left for the engine to run.
fn is_temporal_type(target_type: &str) -> bool {
	let lower = target_type.to_ascii_lowercase();
	let base = lower.split('(').next().unwrap_or(&lower).trim();
	base.starts_with("date") || base.starts_with("time") || base.starts_with("interval")
}

/// Whether a cast of `lit` to `target_type` is a cast of a number, or of the text of one, to
/// `numeric` or `decimal`. The engine runs it, to a `decimal` or a `bigdecimal` by the declared
/// precision, so its digits are kept exactly rather than folded through a float. Text that is not
/// a number is still folded, as MySQL reads its leading number.
fn is_exact_numeric_cast(lit: &BoundLiteral, target_type: &str) -> bool {
	let lower = target_type.to_ascii_lowercase();
	let base = lower.split('(').next().unwrap_or(&lower).trim();
	if !matches!(base, "numeric" | "decimal" | "dec") {
		return false;
	}
	match lit {
		BoundLiteral::Integer(_)
		| BoundLiteral::Unsigned(_)
		| BoundLiteral::Float(_)
		| BoundLiteral::Numeric(_) => true,
		BoundLiteral::String(text) => text.trim().parse::<surrealdb_types::BigDecimal>().is_ok(),
		BoundLiteral::Boolean(_) | BoundLiteral::Null => false,
	}
}

pub fn eval_cast(
	lit: &BoundLiteral,
	target_type: &str,
	policy: &DialectPolicy,
) -> Result<BoundLiteral, PlanError> {
	if matches!(lit, BoundLiteral::Null) {
		return Ok(BoundLiteral::Null);
	}

	let norm_type = target_type.to_lowercase();
	// A `numeric` folds alike whatever its precision and scale.
	let norm_type = match norm_type.split_once('(') {
		Some((base, _)) if matches!(base.trim(), "numeric" | "decimal") => base.trim().to_string(),
		Some(_) | None => norm_type,
	};
	match norm_type.as_str() {
		"text" | "varchar" | "char" | "string" => Ok(BoundLiteral::String(literal_to_string(lit))),
		"unsigned" => match lit {
			BoundLiteral::Integer(i) => Ok(BoundLiteral::Unsigned(*i as u64)),
			BoundLiteral::Unsigned(u) => Ok(BoundLiteral::Unsigned(*u)),
			BoundLiteral::Float(f) => Ok(BoundLiteral::Unsigned(unsigned_of_float(*f))),
			// A decimal rounds to an integer, and a negative one wraps as a negative float does.
			BoundLiteral::Numeric(text) => match rounded_integer(text) {
				Some(int) if int < 0 => i64::try_from(int)
					.map(|signed| BoundLiteral::Unsigned(signed as u64))
					.map_err(|_| PlanError::NumericValueOutOfRange),
				Some(int) => u64::try_from(int)
					.map(BoundLiteral::Unsigned)
					.map_err(|_| PlanError::NumericValueOutOfRange),
				None => Err(PlanError::NumericValueOutOfRange),
			},
			BoundLiteral::Boolean(b) => Ok(BoundLiteral::Unsigned(u64::from(*b))),
			BoundLiteral::String(s) => match s.trim().parse::<u64>() {
				Ok(u) => Ok(BoundLiteral::Unsigned(u)),
				Err(_) => {
					if policy.dialect == SqlDialect::Postgres {
						Err(PlanError::InvalidTextRepresentation(format!("integer: \"{s}\"")))
					} else if let Ok(signed) = s.trim().parse::<i64>() {
						Ok(BoundLiteral::Unsigned(signed as u64))
					} else {
						let (num, _) = mysql_parse_leading_number(s);
						Ok(BoundLiteral::Unsigned(unsigned_of_float(num)))
					}
				}
			},
			BoundLiteral::Null => Ok(BoundLiteral::Null),
		},
		"int" | "integer" | "int2" | "int4" | "int8" | "bigint" | "smallint" | "signed" => {
			match lit {
				BoundLiteral::Integer(i) => Ok(BoundLiteral::Integer(*i)),
				// MySQL reads an unsigned 64-bit value as the signed value with the same bits.
				BoundLiteral::Unsigned(u) => Ok(BoundLiteral::Integer(*u as i64)),
				BoundLiteral::Float(f) => Ok(BoundLiteral::Integer(*f as i64)),
				// A numeric rounds half away from zero.
				BoundLiteral::Numeric(text) => rounded_integer(text)
					.and_then(|int| i64::try_from(int).ok())
					.map(BoundLiteral::Integer)
					.ok_or(PlanError::NumericValueOutOfRange),
				BoundLiteral::Boolean(b) => Ok(BoundLiteral::Integer(if *b {
					1
				} else {
					0
				})),
				BoundLiteral::String(s) => match s.trim().parse::<i64>() {
					Ok(i) => Ok(BoundLiteral::Integer(i)),
					Err(_) => {
						if policy.dialect == SqlDialect::Postgres {
							Err(PlanError::InvalidTextRepresentation(format!("integer: \"{s}\"")))
						} else {
							let (num, _) = mysql_parse_leading_number(s);
							Ok(BoundLiteral::Integer(num as i64))
						}
					}
				},
				BoundLiteral::Null => Ok(BoundLiteral::Null),
			}
		}
		"float" | "float4" | "float8" | "real" | "double" | "numeric" | "decimal" => match lit {
			BoundLiteral::Float(f) => Ok(BoundLiteral::Float(*f)),
			// A numeric beyond the float's range, or nonzero but held by the float as zero, is
			// out of range rather than an infinity or a zero; a `real` holds the narrower range
			// of a single-precision float.
			BoundLiteral::Numeric(text) => {
				let value = numeric_f64(text);
				let single = matches!(norm_type.as_str(), "float4" | "real");
				let limit = if single {
					f64::from(f32::MAX)
				} else {
					f64::MAX
				};
				// A nonzero numeric that the float would hold as zero underflows; a subnormal
				// value is held.
				#[allow(clippy::cast_possible_truncation)]
				let held = if single {
					f64::from(value as f32)
				} else {
					value
				};
				let underflows = numeric_nonzero(text) && held == 0.0;
				if value.is_finite() && value.abs() <= limit && !underflows {
					Ok(BoundLiteral::Float(value))
				} else {
					Err(PlanError::NumericValueOutOfRange)
				}
			}
			BoundLiteral::Integer(i) => Ok(BoundLiteral::Float(*i as f64)),
			BoundLiteral::Unsigned(u) => Ok(BoundLiteral::Float(*u as f64)),
			BoundLiteral::Boolean(b) => Ok(BoundLiteral::Float(if *b {
				1.0
			} else {
				0.0
			})),
			BoundLiteral::String(s) => match s.trim().parse::<f64>() {
				Ok(f) => Ok(BoundLiteral::Float(f)),
				Err(_) => {
					if policy.dialect == SqlDialect::Postgres {
						Err(PlanError::InvalidTextRepresentation(format!("float: \"{s}\"")))
					} else {
						let (num, _) = mysql_parse_leading_number(s);
						Ok(BoundLiteral::Float(num))
					}
				}
			},
			BoundLiteral::Null => Ok(BoundLiteral::Null),
		},
		"bool" | "boolean" => match lit {
			BoundLiteral::Boolean(b) => Ok(BoundLiteral::Boolean(*b)),
			BoundLiteral::Integer(i) => Ok(BoundLiteral::Boolean(*i != 0)),
			BoundLiteral::Unsigned(u) => Ok(BoundLiteral::Boolean(*u != 0)),
			BoundLiteral::Float(f) => Ok(BoundLiteral::Boolean(*f != 0.0)),
			BoundLiteral::Numeric(text) => Ok(BoundLiteral::Boolean(numeric_f64(text) != 0.0)),
			BoundLiteral::String(s) => match s.trim().to_lowercase().as_str() {
				"true" | "t" | "1" | "yes" | "on" => Ok(BoundLiteral::Boolean(true)),
				"false" | "f" | "0" | "no" | "off" => Ok(BoundLiteral::Boolean(false)),
				_ => {
					if policy.dialect == SqlDialect::Postgres {
						Err(PlanError::InvalidTextRepresentation(format!("boolean: \"{s}\"")))
					} else {
						let (num, _) = mysql_parse_leading_number(s);
						Ok(BoundLiteral::Boolean(num != 0.0))
					}
				}
			},
			BoundLiteral::Null => Ok(BoundLiteral::Null),
		},
		_ => Ok(lit.clone()),
	}
}

/// Whether `text` matches the expression built from a `LIKE` pattern. A pattern that is too long
/// for the regular expression engine to compile is an error rather than a mismatch.
pub(crate) fn like_matches(regex: &str, text: &str) -> Result<bool, PlanError> {
	Regex::new(regex)
		.map(|regex| regex.is_match(text))
		.map_err(|_| PlanError::Custom("LIKE pattern is too complex".to_string()))
}

/// Convert SQL LIKE pattern (with `%` and `_`) to regular expression string.
///
/// A backslash makes the character after it literal, so `\%`, `\_` and `\\` stand for a percent
/// sign, an underscore and a backslash, and a backslash before any other character is dropped. A
/// backslash that ends the pattern has nothing to escape and stands for itself. The expression
/// is anchored at both ends and its `.` matches a line break, as `%` and `_` do in both
/// dialects. `lower::sql_like::like_pattern_regex` builds the same text for a pattern known only at
/// run time.
pub fn sql_like_to_regex(pattern: &str, case_insensitive: bool) -> String {
	let mut regex = String::with_capacity(pattern.len() + 7);
	regex.push_str(if case_insensitive {
		"(?is)^"
	} else {
		"(?s)^"
	});
	let mut chars = pattern.chars().peekable();
	while let Some(c) = chars.next() {
		match c {
			'\\' => {
				if let Some(next) = chars.next() {
					regex.push_str(&regex_escape_char(next));
				} else {
					regex.push_str("\\\\");
				}
			}
			'%' => regex.push_str(".*"),
			'_' => regex.push('.'),
			other => regex.push_str(&regex_escape_char(other)),
		}
	}
	regex.push('$');
	regex
}

fn regex_escape_char(c: char) -> String {
	match c {
		'.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' => {
			format!("\\{c}")
		}
		other => other.to_string(),
	}
}

/// The unsigned 64-bit value MySQL gives a number cast to `UNSIGNED`: a negative number is
/// rounded and read as the unsigned value with the same bits, as an integer is.
fn unsigned_of_float(value: f64) -> u64 {
	if value < 0.0 {
		value.round() as i64 as u64
	} else {
		value as u64
	}
}

/// MySQL extracts leading number from string (e.g. `'1abc' -> (1.0, true)`, `'2.5xyz' -> (2.5,
/// false)`).
pub fn mysql_parse_leading_number(s: &str) -> (f64, bool) {
	let trimmed = s.trim();
	if trimmed.is_empty() {
		return (0.0, true);
	}

	let mut chars = trimmed.char_indices().peekable();
	let mut end_idx = 0;
	let mut has_dot = false;

	// Optional leading +/-
	if let Some(&(_, '+' | '-')) = chars.peek() {
		chars.next();
		end_idx = 1;
	}

	let mut found_digit = false;
	while let Some(&(idx, c)) = chars.peek() {
		if c.is_ascii_digit() {
			found_digit = true;
			chars.next();
			end_idx = idx + 1;
		} else if c == '.' && !has_dot {
			has_dot = true;
			chars.next();
			end_idx = idx + 1;
		} else {
			break;
		}
	}

	if !found_digit {
		return (0.0, true);
	}

	let num_str = &trimmed[..end_idx];
	if has_dot {
		let f = num_str.parse::<f64>().unwrap_or(0.0);
		(f, false)
	} else {
		let i = num_str.parse::<i64>().unwrap_or(0);
		(i as f64, true)
	}
}

fn literal_to_bool(lit: &BoundLiteral) -> Option<bool> {
	match lit {
		BoundLiteral::Boolean(b) => Some(*b),
		BoundLiteral::Integer(i) => Some(*i != 0),
		BoundLiteral::Unsigned(u) => Some(*u != 0),
		BoundLiteral::Float(f) => Some(*f != 0.0),
		BoundLiteral::Numeric(text) => Some(numeric_f64(text) != 0.0),
		BoundLiteral::Null => None,
		BoundLiteral::String(s) => match s.trim().to_lowercase().as_str() {
			"true" | "t" | "1" => Some(true),
			"false" | "f" | "0" => Some(false),
			_ => None,
		},
	}
}

fn literal_to_string(lit: &BoundLiteral) -> String {
	match lit {
		BoundLiteral::String(s) => s.clone(),
		BoundLiteral::Integer(i) => i.to_string(),
		BoundLiteral::Unsigned(u) => u.to_string(),
		BoundLiteral::Float(f) => f.to_string(),
		BoundLiteral::Numeric(text) => text.clone(),
		BoundLiteral::Boolean(b) => b.to_string(),
		BoundLiteral::Null => String::new(),
	}
}

/// The exact value of an integer or numeric constant. `None` for any other literal, a float
/// included, whose arithmetic is on doubles.
fn exact_decimal(lit: &BoundLiteral) -> Option<BigDecimal> {
	match lit {
		BoundLiteral::Integer(i) => Some(BigDecimal::from(*i)),
		BoundLiteral::Unsigned(u) => Some(BigDecimal::from(*u)),
		BoundLiteral::Numeric(text) => text.parse::<BigDecimal>().ok(),
		BoundLiteral::Float(_)
		| BoundLiteral::String(_)
		| BoundLiteral::Boolean(_)
		| BoundLiteral::Null => None,
	}
}

/// Whether an operation over a numeric constant is left for the engine rather than folded: a
/// division or a remainder by a divisor that is not zero, and a power, between exact operands. The
/// engine computes them on decimals, to the scale the dialect gives a quotient; a zero divisor
/// still folds to its error. An operation with a float operand is on doubles and always folds, so
/// the numeric's conversion to a double is checked for range.
fn leaves_numeric_to_engine(left: &BoundLiteral, op: BoundBinaryOp, right: &BoundLiteral) -> bool {
	if !matches!(left, BoundLiteral::Numeric(_)) && !matches!(right, BoundLiteral::Numeric(_)) {
		return false;
	}
	if matches!(left, BoundLiteral::Float(_)) || matches!(right, BoundLiteral::Float(_)) {
		return false;
	}
	if matches!(op, BoundBinaryOp::Divide | BoundBinaryOp::Remainder) {
		return !exact_decimal(right).is_some_and(|divisor| divisor.is_zero());
	}
	matches!(op, BoundBinaryOp::Power)
}

/// The most significant digits a PostgreSQL `numeric` holds before the decimal point.
const NUMERIC_MAX_INTEGER_DIGITS: i64 = 131_072;

/// Refuses the exact quotient of two constants, one of them a numeric, that has more integer
/// digits than a PostgreSQL `numeric` holds. The quotient itself is left for the engine; its
/// integer digits follow from the operands' leading digits and their positions.
fn numeric_quotient_in_range(left: &BoundLiteral, right: &BoundLiteral) -> Result<(), PlanError> {
	if !matches!(left, BoundLiteral::Numeric(_)) && !matches!(right, BoundLiteral::Numeric(_)) {
		return Ok(());
	}
	let (Some((dividend, dividend_at)), Some((divisor, divisor_at))) =
		(significant_digits(left), significant_digits(right))
	else {
		return Ok(());
	};
	// The quotient's leading digit sits at the difference of the operands' positions, one place
	// lower when the dividend's digits are the smaller.
	let lower = i64::from(dividend.as_str() < divisor.as_str());
	let leading_at = dividend_at.saturating_sub(divisor_at).saturating_sub(lower);
	if leading_at >= NUMERIC_MAX_INTEGER_DIGITS {
		return Err(PlanError::NumericValueOutOfRange);
	}
	Ok(())
}

/// Refuses the exact power of two constants, one of them a numeric, that has more integer digits
/// than a PostgreSQL `numeric` holds. The power itself is left for the engine; its leading digit
/// sits at the exponent times the base's power of ten, which a double tells closely enough to
/// judge a bound of 131072 digits.
fn numeric_power_in_range(base: &BoundLiteral, exponent: &BoundLiteral) -> Result<(), PlanError> {
	if !matches!(base, BoundLiteral::Numeric(_)) && !matches!(exponent, BoundLiteral::Numeric(_)) {
		return Ok(());
	}
	let (Some((digits, leading_at)), Some(exponent), Some(exact_base)) = (
		significant_digits(base),
		exact_decimal(exponent).map(|e| e.to_f64()),
		exact_decimal(base),
	) else {
		return Ok(());
	};
	// A base near one has a magnitude below what its leading digits tell a double, so it is
	// read from its exact distance to one instead.
	let Some(offset) = exact_base.abs().checked_sub(&BigDecimal::from(1_i64)) else {
		return Ok(());
	};
	if offset.is_zero() {
		return Ok(());
	}
	let offset = offset.to_f64();
	let magnitude = if offset.abs() < 1e-3 {
		offset.ln_1p() / std::f64::consts::LN_10
	} else {
		// A double reads at most 17 significant digits.
		let lead: String = digits.chars().take(17).collect();
		let Ok(value) = lead.parse::<f64>() else {
			return Ok(());
		};
		let places = i32::try_from(lead.len()).unwrap_or(17).saturating_sub(1);
		leading_at as f64 + (value / 10f64.powi(places)).log10()
	};
	if (exponent * magnitude).floor() >= NUMERIC_MAX_INTEGER_DIGITS as f64 {
		return Err(PlanError::NumericValueOutOfRange);
	}
	Ok(())
}

/// The significant digits of an exact constant, with trailing zeros dropped, and the power of ten
/// of its leading digit (`0` for `1.5`, `-1` for `0.25`). `None` for zero and for any literal that
/// is not an integer or a numeric.
fn significant_digits(lit: &BoundLiteral) -> Option<(String, i64)> {
	let text = match lit {
		BoundLiteral::Integer(i) => i.to_string(),
		BoundLiteral::Unsigned(u) => u.to_string(),
		BoundLiteral::Numeric(text) => text.clone(),
		BoundLiteral::Float(_)
		| BoundLiteral::String(_)
		| BoundLiteral::Boolean(_)
		| BoundLiteral::Null => return None,
	};
	let text = text.trim_start_matches(['-', '+']);
	let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((text, "0"));
	let exponent: i64 = exponent.parse().ok()?;
	let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
	let digits = format!("{integer}{fraction}");
	let first = digits.find(|c: char| c != '0')?;
	let integer_len = i64::try_from(integer.len()).ok()?;
	let first_at = i64::try_from(first).ok()?;
	let leading_at =
		integer_len.saturating_sub(1).saturating_sub(first_at).saturating_add(exponent);
	let significant = digits.get(first..)?.trim_end_matches('0').to_owned();
	Some((significant, leading_at))
}

/// The order of two literals of which one is a numeric constant: exactly between integers and
/// numerics, on doubles against a float, and against the leading number of a string in MySQL.
fn compare_numeric(
	left: &BoundLiteral,
	right: &BoundLiteral,
	policy: &DialectPolicy,
) -> Option<std::cmp::Ordering> {
	if let (Some(l), Some(r)) = (exact_decimal(left), exact_decimal(right)) {
		return Some(l.cmp(&r));
	}
	let approximate = |lit: &BoundLiteral| match lit {
		BoundLiteral::Float(f) => Some(*f),
		BoundLiteral::Numeric(text) => Some(numeric_f64(text)),
		BoundLiteral::String(s) if policy.dialect == SqlDialect::MySql => {
			Some(mysql_parse_leading_number(s).0)
		}
		BoundLiteral::Integer(_)
		| BoundLiteral::Unsigned(_)
		| BoundLiteral::String(_)
		| BoundLiteral::Boolean(_)
		| BoundLiteral::Null => None,
	};
	approximate(left)?.partial_cmp(&approximate(right)?)
}

/// `+`, `-`, `*` and `DIV` over a numeric constant and an integer or another numeric, computed
/// exactly. `None` when an operand is not exact, for a zero `DIV` divisor, and for every other
/// operator, which the double-precision rules answer. A sum, difference or product beyond the
/// digits the dialect's exact type holds (PostgreSQL's `numeric`, MySQL's 65-digit `DECIMAL`)
/// is out of range. `DIV` truncates toward zero to a signed 64-bit integer, as MySQL's `DIV`
/// over a decimal operand gives a `BIGINT`.
fn exact_numeric_arithmetic(
	left: &BoundLiteral,
	op: BoundBinaryOp,
	right: &BoundLiteral,
	dialect: SqlDialect,
) -> Option<Result<BoundLiteral, PlanError>> {
	if !matches!(left, BoundLiteral::Numeric(_)) && !matches!(right, BoundLiteral::Numeric(_)) {
		return None;
	}
	let (l, r) = (exact_decimal(left)?, exact_decimal(right)?);
	let numeric = |value: Option<BigDecimal>| {
		value
			.filter(|value| exact_in_range(value, dialect))
			.map(|value| BoundLiteral::Numeric(value.to_string()))
			.ok_or(PlanError::NumericValueOutOfRange)
	};
	if matches!(op, BoundBinaryOp::Add) {
		Some(numeric(l.checked_add(&r)))
	} else if matches!(op, BoundBinaryOp::Subtract) {
		Some(numeric(l.checked_sub(&r)))
	} else if matches!(op, BoundBinaryOp::Multiply) {
		Some(numeric(l.checked_mul(&r)))
	} else if matches!(op, BoundBinaryOp::IntDiv) && !r.is_zero() {
		Some(
			truncated_quotient(&l, &r)
				.and_then(|quotient| quotient.to_i64())
				.map(BoundLiteral::Integer)
				.ok_or(PlanError::NumericValueOutOfRange),
		)
	} else {
		None
	}
}

/// Whether the exact value `value` is one the dialect's exact type holds: PostgreSQL's
/// `numeric`, or MySQL's `DECIMAL` of at most 65 digits.
fn exact_in_range(value: &BigDecimal, dialect: SqlDialect) -> bool {
	match dialect {
		SqlDialect::Postgres => surrealdb_pgsql::parser::expr::numeric_in_range(&value.to_string()),
		SqlDialect::MySql => {
			let scale = usize::try_from(value.scale()).unwrap_or(usize::MAX);
			value.precision().max(scale) <= 65
		}
	}
}

/// `dividend / divisor` truncated toward zero to an integer, exactly. `None` when the divisor is
/// zero or the quotient has more digits than a bigdecimal holds.
fn truncated_quotient(dividend: &BigDecimal, divisor: &BigDecimal) -> Option<BigDecimal> {
	// The quotient rounded to an integer is within one half of the exact quotient, so it is the
	// truncated quotient or one step past it, away from zero.
	let rounded = dividend.div(divisor, 0).ok()?;
	let back = rounded.checked_mul(divisor)?;
	if back.abs() > dividend.abs() {
		rounded.checked_sub(&BigDecimal::from(i64::from(rounded.signum())))
	} else {
		Some(rounded)
	}
}

/// A numeric constant converted to a double, as PostgreSQL converts one implicitly to meet a
/// `float8` operand. A value beyond the double's range, or nonzero but held by a double as zero,
/// is out of range rather than an infinity or a zero.
fn numeric_as_float(text: &str) -> Result<f64, PlanError> {
	let value = numeric_f64(text);
	let underflows = numeric_nonzero(text) && value == 0.0;
	if value.is_finite() && !underflows {
		Ok(value)
	} else {
		Err(PlanError::NumericValueOutOfRange)
	}
}

/// Refuses a comparison of a numeric constant with a float whose conversion of the numeric to a
/// double, which PostgreSQL makes implicitly, is out of range.
fn float_coercion_in_range(left: &BoundLiteral, right: &BoundLiteral) -> Result<(), PlanError> {
	if let (BoundLiteral::Numeric(text), BoundLiteral::Float(_))
	| (BoundLiteral::Float(_), BoundLiteral::Numeric(text)) = (left, right)
	{
		numeric_as_float(text)?;
	}
	Ok(())
}

/// Whether a numeric constant's text has a nonzero digit in its mantissa.
fn numeric_nonzero(text: &str) -> bool {
	let mantissa = text.split_once(['e', 'E']).map_or(text, |(mantissa, _)| mantissa);
	mantissa.bytes().any(|b| b.is_ascii_digit() && b != b'0')
}

/// The nearest double to a numeric constant's text, for the folds that are defined on doubles.
fn numeric_f64(text: &str) -> f64 {
	text.parse::<f64>().unwrap_or(f64::NAN)
}

/// A numeric constant rounded half away from zero to an integer, as a cast to an integer type
/// rounds it. `None` when the text is not a number or the integer exceeds 128 bits.
fn rounded_integer(text: &str) -> Option<i128> {
	let value = text.parse::<BigDecimal>().ok()?;
	value.round(0).ok()?.to_string().parse::<i128>().ok()
}
