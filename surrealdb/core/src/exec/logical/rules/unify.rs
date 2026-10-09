//! One node for every surface form of an array filter or map.
//!
//! A filter over an array is written `v[WHERE p]`, `array::filter(v, |$e| p)` or
//! `v.filter(|$e| p)`; a map is written `array::map(v, |$e| b)` or `v.map(|$e| b)`.
//! Where the forms provably mean the same thing they are one node: the filter
//! forms are [`LogicalExpr::Filter`], the map forms [`LogicalExpr::Broadcast`], so a rule written
//! for the node covers every form.
//!
//! - **Filter.** A closure-written filter whose base is proven an array is the `[WHERE]` filter
//!   over that base, with the closure's parameter replaced by [`LogicalExpr::Current`]. It may
//!   stand at any position.
//! - **Map.** The same shape for `map` is the broadcast over that base, and only at the row: a
//!   broadcast nested in the body of another broadcast does not map again.
//! - **Identity.** A `[WHERE true]` over a proven array is its base.
//!
//! A rewrite happens only where no result and no error can change:
//!
//! - The base is proven an array by what the query computes ([`proven_array`]), never by the row's
//!   schema, so a base that may be anything keeps the call and the error a non-array raises.
//! - The closure binds one parameter of kind `any`, declares no return kind, and does not name a
//!   row parameter ([`binds_one_plain_parameter`]).
//! - The body is built only of forms that read no row and no document, so the element a body reads
//!   as its parameter is the one it reads as the current value ([`admitted_body`]).
//! - The body cannot raise: a body that may raise keeps the call, so the error the closure's
//!   invocation reports is never exchanged for one the `[WHERE]` predicate reports. What may raise
//!   is judged by [`may_raise`], which counts a read of a permission-checked `DEFINE PARAM`, a
//!   closure body's reads included.
//! - The caller may execute the builtin: a denied one keeps its refusal.
//!
//! The rule rewrites the scalars of every operator; a closure body and a nested plan are left as
//! written, a scalar holding a `MATCHES` or KNN operator is left as it is, and so are a `MATCH`
//! plan's scalars.

use super::{ROW_PARAMS, RuleContext, RuleDef, may_raise, rewrite_positions};
use crate::expr::logical::{
	LogicalClosure, LogicalExpr, LogicalLiteral, LogicalOp, OpKind, Schema,
};
use crate::expr::{BinaryOperator, Function, Kind};
use crate::val::Value;

pub(super) const RULE: RuleDef = RuleDef {
	name: "unify",
	// A filter or a map can sit in any node's scalars.
	kinds: OpKind::ALL,
	apply,
};

const ARRAY_FILTER: &str = "array::filter";
const ARRAY_MAP: &str = "array::map";

fn apply(node: &mut LogicalOp, cx: &mut RuleContext<'_>) -> bool {
	if matches!(node, LogicalOp::Match(_)) {
		return false;
	}
	let holds_candidate = node
		.visit_scalars(&mut |scalar| {
			if admits_rewrite(scalar) && scalar.any(is_candidate) {
				Err(())
			} else {
				Ok(())
			}
		})
		.is_err();
	if !holds_candidate {
		return false;
	}
	let mut changed = false;
	let Ok(()) = node.visit_scalars_mut(&mut |scalar| {
		if !admits_rewrite(scalar) {
			return Ok(());
		}
		rewrite_positions(scalar, true, &mut |e, at_row| {
			if let Some(new) = rewrite(e, at_row, cx) {
				*e = new;
				changed = true;
			}
		});
		Ok::<_, std::convert::Infallible>(())
	});
	changed
}

/// Whether `scalar` may be rewritten: one holding a `MATCHES` or a KNN
/// operator is left as written, because the planner reads those operators'
/// operands by shape — a call is a query vector computed once, a broadcast is
/// not — and an access path chosen from the shape would change.
fn admits_rewrite(scalar: &LogicalExpr) -> bool {
	!scalar.any(|e| {
		matches!(
			e,
			LogicalExpr::Binary {
				op: BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_),
				..
			}
		)
	})
}

/// Whether `expr` is a shape [`rewrite`] may rewrite: a two-argument call, a
/// one-argument method, or a `[WHERE true]`.
#[expect(clippy::wildcard_enum_match_arm, reason = "any other expression is not a candidate")]
fn is_candidate(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Call(call) => {
			matches!(&call.receiver, Function::Normal(name) if name == ARRAY_FILTER || name == ARRAY_MAP)
				&& call.args.len() == 2
		}
		LogicalExpr::Method {
			args,
			..
		} => args.len() == 1,
		LogicalExpr::Filter {
			predicate,
			..
		} => is_true(predicate),
		_ => false,
	}
}

/// Which builtin a closure-written form calls.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Builtin {
	Filter,
	Map,
}

impl Builtin {
	fn name(self) -> &'static str {
		match self {
			Builtin::Filter => ARRAY_FILTER,
			Builtin::Map => ARRAY_MAP,
		}
	}
}

/// What `expr` is replaced by, or `None` where it stays.
#[expect(clippy::wildcard_enum_match_arm, reason = "any other expression is not rewritten")]
fn rewrite(expr: &LogicalExpr, at_row: bool, cx: &RuleContext<'_>) -> Option<LogicalExpr> {
	match expr {
		LogicalExpr::Filter {
			base,
			predicate,
		} if is_true(predicate) && proven_array(base) => Some((**base).clone()),
		_ => {
			let (builtin, base, closure) = closure_form(expr, cx)?;
			if builtin == Builtin::Map && !at_row {
				return None;
			}
			if !guards_hold(builtin, base, closure, cx) {
				return None;
			}
			let param = closure.args[0].0.as_str();
			let body = Box::new(current_for(&closure.body, param));
			let base = Box::new(base.clone());
			Some(match builtin {
				Builtin::Filter => LogicalExpr::Filter {
					base,
					predicate: body,
				},
				Builtin::Map => LogicalExpr::Broadcast {
					base,
					body,
					flatten: false,
				},
			})
		}
	}
}

/// The builtin `expr` calls, its base and its closure, when `expr` is
/// `array::filter(base, |…| …)` or `array::map(base, |…| …)`, or the method
/// form of either on an array receiver.
#[expect(clippy::wildcard_enum_match_arm, reason = "any other expression is not a closure form")]
fn closure_form<'e>(
	expr: &'e LogicalExpr,
	cx: &RuleContext<'_>,
) -> Option<(Builtin, &'e LogicalExpr, &'e LogicalClosure)> {
	match expr {
		LogicalExpr::Call(call) => {
			let Function::Normal(name) = &call.receiver else {
				return None;
			};
			let builtin = match name.as_str() {
				ARRAY_FILTER => Builtin::Filter,
				ARRAY_MAP => Builtin::Map,
				_ => return None,
			};
			let [base, LogicalExpr::Closure(closure)] = call.args.as_slice() else {
				return None;
			};
			Some((builtin, base, closure))
		}
		LogicalExpr::Method {
			base,
			name,
			args,
		} => {
			let [LogicalExpr::Closure(closure)] = args.as_slice() else {
				return None;
			};
			let descriptor = cx.registry.get_method(name.as_str())?;
			let resolved = descriptor.resolve(&Value::Array(Default::default()))?.name();
			let builtin = match resolved {
				ARRAY_FILTER => Builtin::Filter,
				ARRAY_MAP => Builtin::Map,
				_ => return None,
			};
			Some((builtin, base, closure))
		}
		_ => None,
	}
}

/// Whether every guard that keeps the rewrite from changing a result or an
/// error holds for `builtin` over `base` with `closure`.
fn guards_hold(
	builtin: Builtin,
	base: &LogicalExpr,
	closure: &LogicalClosure,
	cx: &RuleContext<'_>,
) -> bool {
	proven_array(base)
		&& binds_one_plain_parameter(closure)
		&& admitted_body(&closure.body, cx)
		&& body_cannot_raise(&closure.body, cx)
		&& builtin_allowed(builtin, cx)
}

/// Whether `expr` is an array by what the query computes: an array
/// constructor, or an expression checked to be of an array kind, such as a
/// cast. The row's schema is not consulted.
fn proven_array(expr: &LogicalExpr) -> bool {
	matches!(expr, LogicalExpr::Array(_))
		|| matches!(expr.checked_kind(&Schema::open()), Some(Kind::Array(..)))
}

/// Whether `closure` binds exactly one parameter of kind `any`, not named for
/// a row parameter, and declares no return kind.
fn binds_one_plain_parameter(closure: &LogicalClosure) -> bool {
	let [(param, kind)] = closure.args.as_slice() else {
		return false;
	};
	matches!(kind, Kind::Any) && closure.returns.is_none() && !ROW_PARAMS.contains(&param.as_str())
}

/// Whether `body` is built only of literals, collection constructors,
/// constants, operators, `IF`, parameters other than the row's, and calls to
/// pure builtins that are neither index nor projection functions: forms that
/// read neither the row nor the document and run no nested body or plan.
fn admitted_body(body: &LogicalExpr, cx: &RuleContext<'_>) -> bool {
	!body.any(|node| !admits(node, cx))
}

/// Whether `node` itself, not its children, is a form [`admitted_body`]
/// allows.
#[expect(clippy::wildcard_enum_match_arm, reason = "every other form is rejected")]
fn admits(node: &LogicalExpr, cx: &RuleContext<'_>) -> bool {
	match node {
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::If(_) => true,
		LogicalExpr::Param(param) => !ROW_PARAMS.contains(&param.as_str()),
		LogicalExpr::Call(call) => match &call.receiver {
			Function::Normal(name) => {
				cx.registry.get(name).is_some_and(|f| f.is_pure())
					&& !cx.registry.is_index_function(name)
					&& !cx.registry.is_projection(name)
			}
			_ => false,
		},
		_ => false,
	}
}

/// Whether evaluating `body` cannot raise, so that the rewrite never replaces
/// an error of the closure's invocation with one of the filter's predicate.
fn body_cannot_raise(body: &LogicalExpr, cx: &RuleContext<'_>) -> bool {
	!may_raise(body, cx.permission_checked_params)
}

/// Whether the caller may execute `builtin`; a denied one keeps its refusal.
fn builtin_allowed(builtin: Builtin, cx: &RuleContext<'_>) -> bool {
	cx.capabilities.allows_function_name(builtin.name())
}

/// `body` with every read of parameter `param` replaced by the current value.
/// The body holds no nested body, so a whole-tree replacement is faithful.
fn current_for(body: &LogicalExpr, param: &str) -> LogicalExpr {
	let mut out = body.clone();
	let Ok(()) = out.visit_mut(&mut |node| {
		if matches!(node, LogicalExpr::Param(p) if p.as_str() == param) {
			*node = LogicalExpr::Current;
		}
		Ok::<_, std::convert::Infallible>(())
	});
	out
}

/// Whether `expr` is the literal `true`.
fn is_true(expr: &LogicalExpr) -> bool {
	matches!(expr, LogicalExpr::Literal(LogicalLiteral::Bool(true)))
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::super::super::lower_test as lower;
	use super::super::test_support::{rewritten, rewritten_capable, rewritten_checking};
	use crate::dbs::capabilities::{Capabilities, Targets};
	use crate::expr::BinaryOperator;
	use crate::expr::logical::{LogicalExpr, LogicalLiteral, LogicalOp};

	/// The one scalar `op` holds, in `op` or in a plan nested in it. Panics
	/// unless there is exactly one.
	fn scalar_of(op: &LogicalOp) -> LogicalExpr {
		let mut found = Vec::new();
		let Ok(()) = op.visit_scalars_deep(&mut |scalar| {
			found.push(scalar.clone());
			Ok::<_, std::convert::Infallible>(())
		});
		assert_eq!(found.len(), 1, "expected one scalar in\n{op}");
		found.remove(0)
	}

	fn rewritten_scalar(sql: &str) -> LogicalExpr {
		scalar_of(&rewritten(lower(sql)))
	}

	/// Whether `sql` keeps its call or method, whole.
	fn is_left_alone(sql: &str) -> bool {
		matches!(rewritten_scalar(sql), LogicalExpr::Call(_) | LogicalExpr::Method { .. })
	}

	/// A closure-written filter, as a call and as a method, is the `[WHERE]`
	/// filter over its base with the parameter replaced by the current value.
	#[test]
	fn a_closure_filter_over_an_array_is_the_where_filter() {
		let call = rewritten(lower("RETURN array::filter(<array> $a, |$v| $v > 1)"));
		let method = rewritten(lower("RETURN (<array> $a).filter(|$v| $v > 1)"));
		assert_eq!(call, method, "both surface forms reach one plan");
		let LogicalExpr::Filter {
			base,
			predicate,
		} = scalar_of(&call)
		else {
			panic!("expected a filter in\n{call}");
		};
		assert!(matches!(*base, LogicalExpr::Prefix { ref expr, .. }
			if matches!(**expr, LogicalExpr::Param(_))));
		assert!(matches!(*predicate, LogicalExpr::Binary {
			ref left,
			op: BinaryOperator::MoreThan,
			ref right,
		} if matches!(**left, LogicalExpr::Current)
			&& matches!(**right, LogicalExpr::Literal(LogicalLiteral::Integer(1)))));
	}

	/// An array constructor is a proven array.
	#[test]
	fn a_closure_filter_over_an_array_literal_is_a_filter() {
		let LogicalExpr::Filter {
			base,
			..
		} = rewritten_scalar("RETURN array::filter([1, 2, 3], |$v| $v > 1)")
		else {
			panic!("expected a filter");
		};
		assert!(matches!(*base, LogicalExpr::Array(_)));
	}

	/// Every filter form over a proven array that admits every element is the
	/// array.
	#[test]
	fn every_filter_form_that_admits_everything_is_its_base() {
		let base = rewritten(lower("RETURN <array> $a"));
		for sql in [
			"RETURN (<array> $a)[WHERE true]",
			"RETURN array::filter(<array> $a, |$v| true)",
			"RETURN (<array> $a).filter(|$v| true)",
		] {
			assert_eq!(rewritten(lower(sql)), base, "`{sql}`");
		}
	}

	/// A closure-written map, as a call and as a method, is the broadcast of its
	/// body over its base.
	#[test]
	fn a_closure_map_over_an_array_is_a_broadcast() {
		let call = rewritten(lower("RETURN array::map(<array> $a, |$v| [$v])"));
		let method = rewritten(lower("RETURN (<array> $a).map(|$v| [$v])"));
		assert_eq!(call, method, "both surface forms reach one plan");
		let LogicalExpr::Broadcast {
			base,
			body,
			flatten,
		} = scalar_of(&call)
		else {
			panic!("expected a broadcast in\n{call}");
		};
		assert!(!flatten);
		assert!(matches!(*base, LogicalExpr::Prefix { ref expr, .. }
			if matches!(**expr, LogicalExpr::Param(_))));
		assert!(matches!(*body, LogicalExpr::Array(ref items)
			if matches!(items.as_slice(), [LogicalExpr::Current])));
	}

	/// A base that is not proven an array keeps the call.
	#[test]
	fn an_unproven_base_keeps_the_call() {
		assert!(is_left_alone("RETURN array::filter($a, |$v| $v > 1)"));
		assert!(is_left_alone("RETURN $a.filter(|$v| $v > 1)"));
		assert!(is_left_alone("RETURN array::map($a, |$v| [$v])"));
	}

	/// A closure that declares a parameter kind, a return kind or a second
	/// parameter keeps the call.
	#[test]
	fn a_closure_with_declared_kinds_or_two_parameters_keeps_the_call() {
		assert!(is_left_alone("RETURN array::filter(<array> $a, |$v: int| $v > 1)"));
		assert!(is_left_alone("RETURN array::filter(<array> $a, |$v| -> bool { $v > 1 })"));
		assert!(is_left_alone("RETURN array::map(<array> $a, |$v, $i| [$v, $i])"));
	}

	/// A body that reads the row keeps the call: inside a closure a bare field
	/// is the caller's, inside a filter or a broadcast it is the element's.
	#[test]
	fn a_body_that_reads_the_row_keeps_the_call() {
		assert!(is_left_alone("RETURN array::filter(<array> $a, |$v| $v > n)"));
		assert!(is_left_alone("RETURN array::filter(<array> $a, |$v| $v > $this)"));
		assert!(is_left_alone("RETURN array::filter(<array> $a, |$this| $this > 1)"));
		assert!(is_left_alone("RETURN array::filter(<array> $a, |$v| $v > $parent)"));
	}

	/// A body that may raise keeps the call: the method form reports the error
	/// as the method's.
	#[test]
	fn a_body_that_may_raise_keeps_the_call() {
		assert!(is_left_alone("RETURN array::filter(<array> $a, |$v| <int> $v)"));
		assert!(is_left_alone("RETURN (<array> $a).filter(|$v| $v + 1)"));
	}

	/// A body that reads a permission-checked `DEFINE PARAM` may raise, so the
	/// call stays; a name the catalog does not gate does not keep it.
	#[test]
	fn a_body_that_reads_a_permission_checked_param_keeps_the_call() {
		let sql = "RETURN array::filter(<array> $a, |$v| $v > $secret)";
		let kept = rewritten_checking(lower(sql), &["secret"]);
		assert!(matches!(scalar_of(&kept), LogicalExpr::Call(_)), "{kept}");
		let rewritten = rewritten_checking(lower(sql), &["other"]);
		assert!(matches!(scalar_of(&rewritten), LogicalExpr::Filter { .. }), "{rewritten}");
	}

	/// A body that is a block, a nested closure or a nested plan keeps the call.
	#[test]
	fn a_body_that_is_not_a_plain_expression_keeps_the_call() {
		assert!(is_left_alone("RETURN array::filter(<array> $a, |$v| { $v > 1 })"));
		assert!(is_left_alone("RETURN array::filter(<array> $a, |$v| (SELECT VALUE 1 FROM t))"));
		assert!(is_left_alone("RETURN array::filter(<array> $a, |$v| |$w| $w)"));
	}

	/// A builtin the caller may not execute keeps its call, and so its refusal.
	#[test]
	fn a_denied_builtin_keeps_the_call() {
		let sql = "RETURN array::filter([1, 2], |$v| $v > 1)";
		let denied = Capabilities::all().with_functions(Targets::None);
		assert!(matches!(scalar_of(&rewritten_capable(lower(sql), &denied)), LogicalExpr::Call(_)));
		assert!(matches!(
			scalar_of(&rewritten_capable(lower(sql), &Capabilities::all())),
			LogicalExpr::Filter { .. }
		));
	}

	/// A map where the broadcast would not map again keeps the call.
	#[test]
	fn a_map_in_a_body_keeps_the_call() {
		let plan = rewritten(lower("RETURN [[1]][WHERE array::map(<array> $a, |$v| [$v])]"));
		let scalar = scalar_of(&plan);
		assert!(scalar.any(|e| matches!(e, LogicalExpr::Call(_))), "{plan}");
		assert!(!scalar.any(|e| matches!(e, LogicalExpr::Broadcast { .. })), "{plan}");
	}

	/// A `[WHERE true]` over a base not proven an array stays.
	#[test]
	fn a_where_true_over_an_unproven_base_stays() {
		assert!(matches!(rewritten_scalar("RETURN $a[WHERE true]"), LogicalExpr::Filter { .. }));
	}

	/// A call that is the operand of a KNN operator stays: the planner reads
	/// the operand by shape.
	#[test]
	fn a_knn_operand_keeps_the_call() {
		let plan = rewritten(lower(
			"SELECT id FROM t WHERE point <|2,EUCLIDEAN|> array::map(['a'], |$x| $x)",
		))
		.to_string();
		assert!(plan.contains("array::map"), "{plan}");
	}
}
