use crate::expr::function::Function;
use crate::expr::limit::Limit;
use crate::expr::operator::BinaryOperator;
use crate::expr::part::Part;
use crate::expr::visit::{MutVisitor, VisitMut};
use crate::expr::{Expr, Literal};

/// Rewrites `count(->edge) > 0` and `count(->edge) >= 1` patterns to inject
/// `LIMIT 1` into the graph traversal lookup. This short-circuits edge
/// iteration after the first match, turning an O(edges) scan into O(1).
///
/// The rewrite is safe because `count(array) > 0` is equivalent to
/// `count(array LIMIT 1) > 0` — we only need to know if at least one
/// truthy element exists.
pub(crate) struct CountExistsRewriter;

impl MutVisitor for CountExistsRewriter {
	type Error = ();

	fn visit_mut_expr(&mut self, e: &mut Expr) -> Result<(), Self::Error> {
		if let Expr::Binary {
			left,
			op,
			right,
		} = e
		{
			if is_count_exists_pattern(left, op, right) {
				inject_limit_one(left);
			}
			// Also check the reversed form: 0 < count(->edge)
			if is_reverse_count_exists_pattern(left, op, right) {
				inject_limit_one(right);
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

/// Detect `count(idiom_with_lookup) > 0` or `count(idiom_with_lookup) >= 1`
fn is_count_exists_pattern(left: &Expr, op: &BinaryOperator, right: &Expr) -> bool {
	if !is_count_of_graph_traversal(left) {
		return false;
	}
	match (op, right) {
		(BinaryOperator::MoreThan, Expr::Literal(Literal::Integer(0))) => true,
		(BinaryOperator::MoreThanEqual, Expr::Literal(Literal::Integer(n))) if *n <= 1 => true,
		_ => false,
	}
}

/// Detect `0 < count(idiom_with_lookup)` or `1 <= count(idiom_with_lookup)`
fn is_reverse_count_exists_pattern(left: &Expr, op: &BinaryOperator, right: &Expr) -> bool {
	if !is_count_of_graph_traversal(right) {
		return false;
	}
	match (op, left) {
		(BinaryOperator::LessThan, Expr::Literal(Literal::Integer(0))) => true,
		(BinaryOperator::LessThanEqual, Expr::Literal(Literal::Integer(n))) if *n <= 1 => true,
		_ => false,
	}
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

/// Inject `LIMIT 1` into the last `Part::Lookup` in the count's idiom argument
fn inject_limit_one(count_expr: &mut Expr) {
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
				lookup.limit = Some(Limit(Expr::Literal(Literal::Integer(1))));
			}
			break;
		}
	}
}
