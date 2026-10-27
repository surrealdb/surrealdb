//! SQL semantics layer: constant folding, 3VL logic, NULL propagation,
//! dialect-aware operators, division rules, and type coercion.

use regex::Regex;

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

			if let (BoundExpr::Literal(l), BoundExpr::Literal(r)) = (&folded_left, &folded_right) {
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
			// A cast to an array type is read when the expression is lowered, which knows the
			// element type, and so is a cast to `vector`, whose text form the lowering knows;
			// there is no literal to fold either to.
			if let BoundExpr::Literal(lit) = &folded_inner
				&& !target_type.ends_with("[]")
				&& !is_temporal_type(target_type)
				&& !target_type.starts_with("vector")
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
			_ => Ok(BoundLiteral::Null),
		},
		BoundUnaryOp::Positive => match lit {
			BoundLiteral::Null => Ok(BoundLiteral::Null),
			BoundLiteral::Integer(i) => Ok(BoundLiteral::Integer(*i)),
			BoundLiteral::Float(f) => Ok(BoundLiteral::Float(*f)),
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
			BoundLiteral::Float(f) => Ok(BoundLiteral::Float(-*f)),
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
				let eq = literals_equal(l, r, policy);
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
				let eq = literals_equal(l, r, policy);
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
/// to a negative power, overflow).
fn eval_power(left: &BoundLiteral, right: &BoundLiteral) -> Result<BoundLiteral, PlanError> {
	let operand = |lit: &BoundLiteral| match lit {
		BoundLiteral::Integer(i) => Ok(*i as f64),
		BoundLiteral::Float(f) => Ok(*f),
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

fn literals_equal(left: &BoundLiteral, right: &BoundLiteral, policy: &DialectPolicy) -> bool {
	compare_literals(left, right, policy) == Some(std::cmp::Ordering::Equal)
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
		BoundLiteral::Float(f) => Ok(Number::Float(*f)),
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
		if literals_equal(expr, item, policy) {
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

/// Evaluate type cast.
/// Whether a cast to the type yields a date or a time, which no literal is: the cast is left for
/// the engine to run.
fn is_temporal_type(target_type: &str) -> bool {
	let lower = target_type.to_ascii_lowercase();
	let base = lower.split('(').next().unwrap_or(&lower).trim();
	base.starts_with("date") || base.starts_with("timestamp")
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
	match norm_type.as_str() {
		"text" | "varchar" | "char" | "string" => Ok(BoundLiteral::String(literal_to_string(lit))),
		"int" | "integer" | "int2" | "int4" | "int8" | "bigint" | "smallint" | "signed"
		| "unsigned" => match lit {
			BoundLiteral::Integer(i) => Ok(BoundLiteral::Integer(*i)),
			BoundLiteral::Float(f) => Ok(BoundLiteral::Integer(*f as i64)),
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
		},
		"float" | "float4" | "float8" | "real" | "double" | "numeric" | "decimal" => match lit {
			BoundLiteral::Float(f) => Ok(BoundLiteral::Float(*f)),
			BoundLiteral::Integer(i) => Ok(BoundLiteral::Float(*i as f64)),
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
			BoundLiteral::Float(f) => Ok(BoundLiteral::Boolean(*f != 0.0)),
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
		BoundLiteral::Float(f) => Some(*f != 0.0),
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
		BoundLiteral::Float(f) => f.to_string(),
		BoundLiteral::Boolean(b) => b.to_string(),
		BoundLiteral::Null => String::new(),
	}
}
