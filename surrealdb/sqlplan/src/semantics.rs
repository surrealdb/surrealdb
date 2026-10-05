//! SQL semantics layer: constant folding, 3VL logic, NULL propagation,
//! dialect-aware operators, division rules, and type coercion.

use regex::Regex;

use crate::dialect::{DialectPolicy, SqlDialect};
use crate::error::PlanError;
use crate::ir::{
	BoundBinaryOp, BoundExpr, BoundLiteral, BoundSelect, BoundTableSource, BoundUnaryOp,
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
		_ => Ok(()),
	}
}

/// Recursively fold and evaluate constant expressions in `BoundExpr`.
pub fn fold_expr(expr: &BoundExpr, policy: &DialectPolicy) -> Result<BoundExpr, PlanError> {
	match expr {
		BoundExpr::Literal(_)
		| BoundExpr::Column(_)
		| BoundExpr::Param {
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
				match op {
					BoundBinaryOp::And => {
						if let BoundExpr::Literal(BoundLiteral::Boolean(false)) = folded_left {
							return Ok(BoundExpr::Literal(BoundLiteral::Boolean(false)));
						}
						if let BoundExpr::Literal(BoundLiteral::Boolean(false)) = folded_right {
							return Ok(BoundExpr::Literal(BoundLiteral::Boolean(false)));
						}
					}
					BoundBinaryOp::Or => {
						if let BoundExpr::Literal(BoundLiteral::Boolean(true)) = folded_left {
							return Ok(BoundExpr::Literal(BoundLiteral::Boolean(true)));
						}
						if let BoundExpr::Literal(BoundLiteral::Boolean(true)) = folded_right {
							return Ok(BoundExpr::Literal(BoundLiteral::Boolean(true)));
						}
					}
					_ => {}
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
					.map(|item| match item {
						BoundExpr::Literal(l) => l.clone(),
						_ => unreachable!(),
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
			if let BoundExpr::Literal(lit) = &folded_inner {
				let result = eval_cast(lit, target_type, policy)?;
				Ok(BoundExpr::Literal(result))
			} else {
				Ok(BoundExpr::Cast {
					expr: Box::new(folded_inner),
					target_type: target_type.clone(),
				})
			}
		}
	}
}

/// Evaluate constant unary operation.
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
			_ => Ok(BoundLiteral::Null),
		},
		BoundUnaryOp::Negate => match lit {
			BoundLiteral::Null => Ok(BoundLiteral::Null),
			BoundLiteral::Integer(i) => Ok(BoundLiteral::Integer(-*i)),
			BoundLiteral::Float(f) => Ok(BoundLiteral::Float(-*f)),
			BoundLiteral::String(s) if policy.dialect == SqlDialect::MySql => {
				let (num, is_int) = mysql_parse_leading_number(s);
				if is_int {
					Ok(BoundLiteral::Integer(-(num as i64)))
				} else {
					Ok(BoundLiteral::Float(-num))
				}
			}
			_ => Ok(BoundLiteral::Null),
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
	if matches!(op, BoundBinaryOp::Like | BoundBinaryOp::ILike | BoundBinaryOp::Regexp) {
		let text = literal_to_string(left);
		let pattern = literal_to_string(right);

		let matched = match op {
			BoundBinaryOp::Like => {
				let case_insensitive = policy.dialect == SqlDialect::MySql;
				let regex_str = sql_like_to_regex(&pattern, case_insensitive);
				Regex::new(&regex_str).map(|r| r.is_match(&text)).unwrap_or(false)
			}
			BoundBinaryOp::ILike => {
				let regex_str = sql_like_to_regex(&pattern, true);
				Regex::new(&regex_str).map(|r| r.is_match(&text)).unwrap_or(false)
			}
			BoundBinaryOp::Regexp => {
				Regex::new(&pattern).map(|r| r.is_match(&text)).unwrap_or(false)
			}
			_ => unreachable!(),
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

	// 9. Arithmetic operations (`+`, `-`, `*`, `/`, `%`, `DIV`)
	eval_arithmetic(left, op, right, policy)
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
		_ => unreachable!(),
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

/// Arithmetic evaluation with dialect division rules and string-to-number coercion.
fn eval_arithmetic(
	left: &BoundLiteral,
	op: BoundBinaryOp,
	right: &BoundLiteral,
	policy: &DialectPolicy,
) -> Result<BoundLiteral, PlanError> {
	let (l_num, l_is_int) = to_number(left, policy)?;
	let (r_num, r_is_int) = to_number(right, policy)?;

	match op {
		BoundBinaryOp::Add => {
			if l_is_int && r_is_int {
				Ok(BoundLiteral::Integer(l_num as i64 + r_num as i64))
			} else {
				Ok(BoundLiteral::Float(l_num + r_num))
			}
		}
		BoundBinaryOp::Subtract => {
			if l_is_int && r_is_int {
				Ok(BoundLiteral::Integer(l_num as i64 - r_num as i64))
			} else {
				Ok(BoundLiteral::Float(l_num - r_num))
			}
		}
		BoundBinaryOp::Multiply => {
			if l_is_int && r_is_int {
				Ok(BoundLiteral::Integer(l_num as i64 * r_num as i64))
			} else {
				Ok(BoundLiteral::Float(l_num * r_num))
			}
		}
		BoundBinaryOp::Divide => {
			// Check division by zero
			if r_num == 0.0 {
				if policy.dialect == SqlDialect::Postgres {
					return Err(PlanError::DivisionByZero);
				} else {
					return Ok(BoundLiteral::Null);
				}
			}

			if policy.dialect == SqlDialect::Postgres {
				// In PostgreSQL: integer / integer produces integer division (1 / 2 = 0)
				if l_is_int && r_is_int {
					Ok(BoundLiteral::Integer(l_num as i64 / r_num as i64))
				} else {
					Ok(BoundLiteral::Float(l_num / r_num))
				}
			} else {
				// In MySQL: `/` is always float division (1 / 2 = 0.5)
				Ok(BoundLiteral::Float(l_num / r_num))
			}
		}
		BoundBinaryOp::IntDiv => {
			// MySQL integer division (`DIV`)
			if r_num == 0.0 {
				Ok(BoundLiteral::Null)
			} else {
				Ok(BoundLiteral::Integer(l_num as i64 / r_num as i64))
			}
		}
		BoundBinaryOp::Remainder => {
			if r_num == 0.0 {
				if policy.dialect == SqlDialect::Postgres {
					return Err(PlanError::DivisionByZero);
				} else {
					return Ok(BoundLiteral::Null);
				}
			}
			if l_is_int && r_is_int {
				Ok(BoundLiteral::Integer(l_num as i64 % r_num as i64))
			} else {
				Ok(BoundLiteral::Float(l_num % r_num))
			}
		}
		_ => Err(PlanError::unsupported(format!("Arithmetic operation {op:?}"))),
	}
}

/// Convert a literal to a number (f64, is_integer).
fn to_number(lit: &BoundLiteral, policy: &DialectPolicy) -> Result<(f64, bool), PlanError> {
	match lit {
		BoundLiteral::Integer(i) => Ok((*i as f64, true)),
		BoundLiteral::Float(f) => Ok((*f, false)),
		BoundLiteral::String(s) if policy.dialect == SqlDialect::MySql => {
			Ok(mysql_parse_leading_number(s))
		}
		BoundLiteral::String(s) => {
			if let Ok(i) = s.parse::<i64>() {
				Ok((i as f64, true))
			} else if let Ok(f) = s.parse::<f64>() {
				Ok((f, false))
			} else {
				Err(PlanError::InvalidTextRepresentation(format!("numeric: \"{s}\"")))
			}
		}
		BoundLiteral::Boolean(b) => Ok((
			if *b {
				1.0
			} else {
				0.0
			},
			true,
		)),
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

/// Convert SQL LIKE pattern (with `%` and `_`) to regular expression string.
pub fn sql_like_to_regex(pattern: &str, case_insensitive: bool) -> String {
	let mut regex = String::with_capacity(pattern.len() + 6);
	if case_insensitive {
		regex.push_str("(?i)");
	}
	regex.push('^');
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
