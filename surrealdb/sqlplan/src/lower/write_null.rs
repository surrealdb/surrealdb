//! SQL NULL semantics for expressions evaluated by the write engine.

use surrealdb_expr::expr::statements::{IfelseStatement, SetStatement};
use surrealdb_expr::expr::{
	BinaryOperator, Block, Expr, Function, FunctionCall, Idiom, Literal, Param, Part,
	PrefixOperator,
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
		Expr::Table(_)
		| Expr::Mock(_)
		| Expr::Block(_)
		| Expr::Constant(_)
		| Expr::Prefix {
			..
		}
		| Expr::Postfix {
			..
		}
		| Expr::Binary {
			..
		}
		| Expr::FunctionCall(_)
		| Expr::Closure(_)
		| Expr::Break
		| Expr::Continue
		| Expr::Return(_)
		| Expr::Throw(_)
		| Expr::IfElse(_)
		| Expr::Select(_)
		| Expr::Create(_)
		| Expr::Update(_)
		| Expr::Upsert(_)
		| Expr::Delete(_)
		| Expr::Relate(_)
		| Expr::Insert(_)
		| Expr::Define(_)
		| Expr::Remove(_)
		| Expr::Rebuild(_)
		| Expr::Alter(_)
		| Expr::Info(_)
		| Expr::Foreach(_)
		| Expr::Let(_)
		| Expr::Sleep(_)
		| Expr::Explain {
			..
		} => false,
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
				BinaryOperator::Subtract
				| BinaryOperator::Add
				| BinaryOperator::Multiply
				| BinaryOperator::Divide
				| BinaryOperator::Remainder
				| BinaryOperator::Power
				| BinaryOperator::Equal
				| BinaryOperator::ExactEqual
				| BinaryOperator::NotEqual
				| BinaryOperator::AllEqual
				| BinaryOperator::AnyEqual
				| BinaryOperator::NullCoalescing
				| BinaryOperator::TenaryCondition
				| BinaryOperator::LessThan
				| BinaryOperator::LessThanEqual
				| BinaryOperator::MoreThan
				| BinaryOperator::MoreThanEqual
				| BinaryOperator::Contain
				| BinaryOperator::NotContain
				| BinaryOperator::ContainAll
				| BinaryOperator::ContainAny
				| BinaryOperator::ContainNone
				| BinaryOperator::Inside
				| BinaryOperator::NotInside
				| BinaryOperator::AllInside
				| BinaryOperator::AnyInside
				| BinaryOperator::NoneInside
				| BinaryOperator::Outside
				| BinaryOperator::Intersects
				| BinaryOperator::Range
				| BinaryOperator::RangeInclusive
				| BinaryOperator::RangeSkip
				| BinaryOperator::RangeSkipInclusive
				| BinaryOperator::Matches(_)
				| BinaryOperator::NearestNeighbor(_) => None,
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
		other @ Expr::Literal(_)
		| other @ Expr::Param(_)
		| other @ Expr::Idiom(_)
		| other @ Expr::Table(_)
		| other @ Expr::Mock(_)
		| other @ Expr::Block(_)
		| other @ Expr::Constant(_)
		| other @ Expr::Prefix {
			..
		}
		| other @ Expr::Postfix {
			..
		}
		| other @ Expr::FunctionCall(_)
		| other @ Expr::Closure(_)
		| other @ Expr::Break
		| other @ Expr::Continue
		| other @ Expr::Return(_)
		| other @ Expr::Throw(_)
		| other @ Expr::IfElse(_)
		| other @ Expr::Select(_)
		| other @ Expr::Create(_)
		| other @ Expr::Update(_)
		| other @ Expr::Upsert(_)
		| other @ Expr::Delete(_)
		| other @ Expr::Relate(_)
		| other @ Expr::Insert(_)
		| other @ Expr::Define(_)
		| other @ Expr::Remove(_)
		| other @ Expr::Rebuild(_)
		| other @ Expr::Alter(_)
		| other @ Expr::Info(_)
		| other @ Expr::Foreach(_)
		| other @ Expr::Let(_)
		| other @ Expr::Sleep(_)
		| other @ Expr::Explain {
			..
		} => other,
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

/// A comparison where one operand is a date, timestamp or datetime by its syntax: a date is
/// compared as the timestamp, and a date or timestamp as the datetime, that the other operand is,
/// as PostgreSQL and MySQL promote them. SurrealQL compares values of different kinds by kind, so
/// the promotion is written into the expression. With `sql_nulls` the comparison follows SQL's
/// three-valued logic; without it, as MySQL's lowering writes it, it is the plain comparison.
pub(super) fn promoting_comparison(
	left: Expr,
	op: BinaryOperator,
	right: Expr,
	sql_nulls: bool,
) -> Expr {
	let is = |kind: &str, value: &Expr| {
		Expr::FunctionCall(Box::new(FunctionCall {
			receiver: Function::Normal(format!("type::is_{kind}")),
			arguments: vec![value.clone()],
		}))
	};
	let convert = |function: &str, value: &Expr| {
		Expr::FunctionCall(Box::new(FunctionCall {
			receiver: Function::Normal(function.to_owned()),
			arguments: vec![value.clone()],
		}))
	};
	let widen = |value: &Expr, other: &Expr| {
		choose(
			vec![
				(
					raw(is("date", value), BinaryOperator::And, is("timestamp", other)),
					convert("type::timestamp", value),
				),
				(
					raw(
						raw(is("date", value), BinaryOperator::Or, is("timestamp", value)),
						BinaryOperator::And,
						is("datetime", other),
					),
					convert("type::datetime", value),
				),
			],
			value.clone(),
		)
	};
	once(left, "_sql_pl", |left| {
		once(right, "_sql_pr", |right| {
			let (l, r) = (widen(&left, &right), widen(&right, &left));
			if sql_nulls {
				binary(l, op, r)
			} else {
				raw(l, op, r)
			}
		})
	})
}
