use crate::expr::function::Function;
use crate::expr::limit::Limit;
use crate::expr::operator::BinaryOperator;
use crate::expr::part::Part;
use crate::expr::visit::{MutVisitor, VisitMut};
use crate::expr::{Expr, Literal};

/// Rewrites `count(->edge) OP N` patterns to inject a minimal `LIMIT` into
/// the graph traversal lookup, short-circuiting edge iteration once enough
/// results have been collected to determine the comparison outcome.
///
/// For example, `count(->edge) > 5` only needs 6 results to decide truth,
/// so `LIMIT 6` is injected. The special case `count(->edge) > 0` becomes
/// `LIMIT 1` (the original "exists" optimisation).
pub(crate) struct CountLimitRewriter;

impl MutVisitor for CountLimitRewriter {
	type Error = ();

	fn visit_mut_expr(&mut self, e: &mut Expr) -> Result<(), Self::Error> {
		if let Expr::Binary {
			left,
			op,
			right,
		} = e
		{
			// count(->edge) OP N
			if let Some(limit) = count_comparison_limit(left, op, right) {
				inject_limit(left, limit);
			}
			// N OP count(->edge)  —  flip the operator to normalise
			if let Some(flipped) = flip_comparison(op)
				&& let Some(limit) = count_comparison_limit(right, &flipped, left)
			{
				inject_limit(right, limit);
			}
		}

		match e {
			Expr::Param(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Closure(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::IfElse(_)
			| Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Upsert(_)
			| Expr::Alter(_)
			| Expr::Info(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Sleep(_) => {}

			_ => {
				e.visit_mut(self)?;
			}
		}
		Ok(())
	}
}

/// Given `count(->edge) OP N`, compute the minimum LIMIT needed to determine
/// the comparison's truth value. Returns `None` when the pattern doesn't match
/// or the optimisation doesn't apply.
fn count_comparison_limit(count_expr: &Expr, op: &BinaryOperator, n_expr: &Expr) -> Option<i64> {
	if !is_count_of_graph_traversal(count_expr) {
		return None;
	}
	let Expr::Literal(Literal::Integer(n)) = n_expr else {
		return None;
	};
	compute_limit(op, *n)
}

/// Derive the smallest LIMIT that preserves the semantics of `count(...) OP n`.
fn compute_limit(op: &BinaryOperator, n: i64) -> Option<i64> {
	let limit = match op {
		BinaryOperator::MoreThan => n.checked_add(1)?,
		BinaryOperator::MoreThanEqual => n,
		BinaryOperator::LessThan => n,
		BinaryOperator::LessThanEqual => n.checked_add(1)?,
		BinaryOperator::Equal | BinaryOperator::ExactEqual => n.checked_add(1)?,
		BinaryOperator::NotEqual => n.checked_add(1)?,
		_ => return None,
	};
	(limit >= 1).then_some(limit)
}

/// Flip a comparison operator so that `N OP count(...)` becomes
/// `count(...) FLIPPED_OP N`. Symmetric operators return themselves.
fn flip_comparison(op: &BinaryOperator) -> Option<BinaryOperator> {
	Some(match op {
		BinaryOperator::LessThan => BinaryOperator::MoreThan,
		BinaryOperator::LessThanEqual => BinaryOperator::MoreThanEqual,
		BinaryOperator::MoreThan => BinaryOperator::LessThan,
		BinaryOperator::MoreThanEqual => BinaryOperator::LessThanEqual,
		BinaryOperator::Equal => BinaryOperator::Equal,
		BinaryOperator::ExactEqual => BinaryOperator::ExactEqual,
		BinaryOperator::NotEqual => BinaryOperator::NotEqual,
		_ => return None,
	})
}

/// Check if an expression is `count(idiom)` where the idiom contains a graph lookup
fn is_count_of_graph_traversal(expr: &Expr) -> bool {
	let Expr::FunctionCall(fc) = expr else {
		return false;
	};
	if !matches!(&fc.receiver, Function::Normal(name) if name == "count") {
		return false;
	}
	if fc.arguments.len() != 1 {
		return false;
	}
	let Expr::Idiom(idiom) = &fc.arguments[0] else {
		return false;
	};
	idiom.0.iter().any(|p| matches!(p, Part::Lookup(_)))
}

/// Inject a `LIMIT` into the last `Part::Lookup` in the count's idiom argument.
/// Skips injection when the lookup already has a limit.
fn inject_limit(count_expr: &mut Expr, limit_value: i64) {
	let Expr::FunctionCall(fc) = count_expr else {
		return;
	};
	if fc.arguments.len() != 1 {
		return;
	}
	let Expr::Idiom(idiom) = &mut fc.arguments[0] else {
		return;
	};
	for part in idiom.0.iter_mut().rev() {
		if let Part::Lookup(lookup) = part {
			if lookup.limit.is_none() {
				lookup.limit = Some(Limit(Expr::Literal(Literal::Integer(limit_value))));
			}
			break;
		}
	}
}
