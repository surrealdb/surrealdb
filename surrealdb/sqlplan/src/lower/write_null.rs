//! SQL NULL semantics for expressions evaluated by the write engine.

use surrealdb_expr::expr::statements::{IfelseStatement, SetStatement};
use surrealdb_expr::expr::{
	BinaryOperator, Block, Expr, Idiom, Literal, Param, Part, PrefixOperator,
};
use surrealdb_strand::Strand;

pub(super) fn raw(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
	Expr::Binary {
		left: Box::new(left),
		op,
		right: Box::new(right),
	}
}

fn null() -> Expr {
	Expr::Literal(Literal::Null)
}
fn boolean(value: bool) -> Expr {
	Expr::Literal(Literal::Bool(value))
}

/// Bind an expression that may be costly or volatile, retaining simple row and parameter reads.
fn once(value: Expr, name: &str, build: impl FnOnce(Expr) -> Expr) -> Expr {
	let simple = match &value {
		Expr::Param(_) => true,
		Expr::Idiom(Idiom(parts)) => parts.iter().all(|part| matches!(part, Part::Field(_))),
		Expr::Literal(Literal::Array(_) | Literal::Object(_)) => false,
		Expr::Literal(_) => true,
		_ => false,
	};
	if simple {
		return build(value);
	}
	Expr::Block(Box::new(Block(vec![
		Expr::Let(Box::new(SetStatement {
			name: Strand::from(name),
			what: value,
			kind: None,
		})),
		build(Expr::Param(Param::from(name.to_owned()))),
	])))
}

fn choose(branches: Vec<(Expr, Expr)>, otherwise: Expr) -> Expr {
	Expr::IfElse(Box::new(IfelseStatement {
		exprs: branches,
		close: Some(otherwise),
	}))
}

fn null_test(value: Expr) -> Expr {
	raw(
		raw(value.clone(), BinaryOperator::Equal, null()),
		BinaryOperator::Or,
		raw(value, BinaryOperator::Equal, Expr::Literal(Literal::None)),
	)
}

pub(super) fn is_null(value: Expr, negated: bool) -> Expr {
	once(value, "_sql_null", |value| {
		let test = null_test(value);
		if negated {
			Expr::Prefix {
				op: PrefixOperator::Not,
				expr: Box::new(test),
			}
		} else {
			test
		}
	})
}

pub(super) fn unary(op: PrefixOperator, value: Expr) -> Expr {
	once(value, "_sql_unary", |value| {
		choose(
			vec![(null_test(value.clone()), null())],
			Expr::Prefix {
				op,
				expr: Box::new(value),
			},
		)
	})
}

pub(super) fn binary(left: Expr, op: BinaryOperator, right: Expr) -> Expr {
	once(left, "_sql_left", |left| {
		once(right, "_sql_right", |right| {
			let any_null =
				raw(null_test(left.clone()), BinaryOperator::Or, null_test(right.clone()));
			let decisive = match &op {
				BinaryOperator::And => Some(false),
				BinaryOperator::Or => Some(true),
				_ => None,
			};
			let result = raw(left.clone(), op, right.clone());
			match decisive {
				Some(decisive) => {
					let either = raw(
						raw(left, BinaryOperator::Equal, boolean(decisive)),
						BinaryOperator::Or,
						raw(right, BinaryOperator::Equal, boolean(decisive)),
					);
					choose(vec![(either, boolean(decisive)), (any_null, null())], result)
				}
				None => choose(vec![(any_null, null())], result),
			}
		})
	})
}

/// An IN list is evaluated once. A successful match takes precedence over NULL list members;
/// an unmatched value with a NULL member is unknown. Negation preserves unknown.
pub(super) fn membership(value: Expr, items: Vec<Expr>, negated: bool) -> Expr {
	if items.is_empty() {
		return boolean(negated);
	}
	once(value, "_sql_value", |value| {
		once(Expr::Literal(Literal::Array(items)), "_sql_items", |items| {
			let contains_null = raw(
				raw(null(), BinaryOperator::Inside, items.clone()),
				BinaryOperator::Or,
				raw(Expr::Literal(Literal::None), BinaryOperator::Inside, items.clone()),
			);
			choose(
				vec![
					(null_test(value.clone()), null()),
					(raw(value, BinaryOperator::Inside, items), boolean(!negated)),
					(contains_null, null()),
				],
				boolean(negated),
			)
		})
	})
}

/// Add SQL NULL semantics to the raw forms emitted by record-key optimizations.
pub(super) fn guard(expr: Expr) -> Expr {
	match expr {
		Expr::Binary {
			left,
			op: op @ (BinaryOperator::Inside | BinaryOperator::NotInside),
			right,
		} if matches!(&*right, Expr::Literal(Literal::Array(_))) => {
			let Expr::Literal(Literal::Array(items)) = *right else {
				unreachable!()
			};
			let negated = matches!(op, BinaryOperator::NotInside);
			membership(*left, items, negated)
		}
		Expr::Binary {
			left,
			op,
			right,
		} => binary(*left, op, *right),
		other => other,
	}
}

pub(super) fn between(value: Expr, low: Expr, high: Expr, negated: bool) -> Expr {
	once(value, "_sql_between", |value| {
		let result = binary(
			binary(value.clone(), BinaryOperator::MoreThanEqual, low),
			BinaryOperator::And,
			binary(value, BinaryOperator::LessThanEqual, high),
		);
		if negated {
			unary(PrefixOperator::Not, result)
		} else {
			result
		}
	})
}

/// Strict numeric functions evaluate each argument once and return NULL for any absent argument.
pub(super) fn strict_call(name: &str, args: Vec<Expr>) -> Expr {
	let count = args.len();
	once(Expr::Literal(Literal::Array(args)), "_sql_args", |values| {
		let refs: Vec<_> = (0..count)
			.map(|index| {
				Expr::Idiom(Idiom(vec![Part::Start(values.clone()), Part::index_int(index as i64)]))
			})
			.collect();
		let nulls = refs
			.iter()
			.cloned()
			.map(null_test)
			.reduce(|left, right| raw(left, BinaryOperator::Or, right))
			.unwrap_or(boolean(false));
		choose(
			vec![(nulls, null())],
			Expr::FunctionCall(Box::new(surrealdb_expr::expr::FunctionCall {
				receiver: surrealdb_expr::expr::Function::Normal(name.to_string()),
				arguments: refs,
			})),
		)
	})
}
