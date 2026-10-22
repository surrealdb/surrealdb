//! Path compilation for the planner.
//!
//! A logical path — a nest of step forms over a root — compiles to one
//! [`Step`] holding the part evaluators in application order, a
//! [`Broadcast`] where the logical plan placed one, or a [`Current`] for the
//! bare current value. The steps of a chain are compiled iteratively, so a
//! long idiom does not recurse once per step; recursion happens only where
//! the logical form nests an expression (a body, an index, a predicate, an
//! argument).
//!
//! Consecutive lookup hops are fused into one [`LookupPart`] whose plan is a
//! single operator chain, with a fast-path-eligible `->edge->vertex` pair
//! collapsed into one scan.
//!
//! A chain of plain field accesses over a constant root (`$session.data.orgs`,
//! `{ a: { b: 1 } }.a.b`) compiles to a [`FoldedPath`] holding the value it
//! reads, so the value is picked once rather than on every row. A bound plan
//! has had its parameters substituted and such chains folded by the logical
//! fold pass before it reaches the planner, so this reaches the scalars
//! compiled outside binding: a stored permission body that a runtime-enforcing
//! scan compiles directly, and the runtime re-entry through `compile_expr`.
//! Access paths are chosen on the logical plan before any scalar is compiled,
//! so nothing folded here changes one; what it removes is the per-row clone of
//! the whole `$session` value.

use super::Planner;
use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::expr::logical::{LogicalDestructureField, LogicalExpr};
use crate::expr::part::Recurse;
use crate::val::Value;

// ============================================================================
// impl Planner — Path Compilation
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// The `(min, max)` depth of a recursion, validated against the
	/// configured idiom recursion limit: the minimum is at least 1 and the
	/// maximum, when given, no more than `idiom_recursion_limit`.
	pub(super) fn recurse_bounds(&self, recurse: &Recurse) -> Result<(u32, Option<u32>), Error> {
		let system_limit = self.ctx.config.exec.idiom_recursion_limit;
		let (min_depth, max_depth) = match *recurse {
			Recurse::Fixed(n) => (n, Some(n)),
			Recurse::Range(min, max) => (min.unwrap_or(1), max),
		};
		if min_depth < 1 {
			return Err(ExecError::InvalidBound {
				found: min_depth.to_string(),
				expected: "at least 1".into(),
			}
			.into());
		}
		if let Some(max) = max_depth
			&& max > system_limit
		{
			return Err(ExecError::InvalidBound {
				found: max.to_string(),
				expected: format!("{} at most", system_limit),
			}
			.into());
		}
		Ok((min_depth, max_depth))
	}
}

// ============================================================================
// Free Functions
// ============================================================================

/// Whether a recursion body re-enters through `.@` anywhere along its path:
/// on the chain of steps itself, inside a broadcast or optional body, inside
/// a destructure alias body, or inside a nested recursion's own body. The
/// expressions a step holds beside its base — an index, a filter predicate,
/// method arguments — are not part of the path and are not searched.
pub(crate) fn contains_repeat_recurse(expr: &LogicalExpr) -> bool {
	fn destructure_contains(fields: &[LogicalDestructureField]) -> bool {
		fields.iter().any(|field| match field {
			LogicalDestructureField::Aliased(_, body) => contains_repeat_recurse(body),
			LogicalDestructureField::Destructure(_, nested) => destructure_contains(nested),
			LogicalDestructureField::All(_)
			| LogicalDestructureField::Field(_)
			| LogicalDestructureField::Rest => false,
		})
	}
	let mut current = expr;
	loop {
		match current {
			LogicalExpr::RepeatRecurse(_) => return true,
			LogicalExpr::Destructure {
				base,
				fields,
			} => {
				if destructure_contains(fields) {
					return true;
				}
				current = base;
			}
			LogicalExpr::Recurse(recurse) => {
				if recurse.path.as_ref().is_some_and(|p| contains_repeat_recurse(p.body())) {
					return true;
				}
				current = &recurse.base;
			}
			LogicalExpr::Optional {
				base,
				then,
			} => {
				if contains_repeat_recurse(then) {
					return true;
				}
				current = base;
			}
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => {
				if contains_repeat_recurse(body) {
					return true;
				}
				current = base;
			}
			LogicalExpr::Access {
				base,
				..
			}
			| LogicalExpr::Index {
				base,
				..
			}
			| LogicalExpr::Flatten {
				base,
				..
			}
			| LogicalExpr::Filter {
				base,
				..
			}
			| LogicalExpr::Method {
				base,
				..
			}
			| LogicalExpr::Lookup {
				base,
				..
			} => current = base,
			LogicalExpr::First(base) | LogicalExpr::Last(base) | LogicalExpr::Each(base) => {
				current = base
			}
			LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => return false,
		}
	}
}

impl Planner<'_> {
	/// The constant value a path's root denotes, when it denotes one.
	///
	/// Two shapes qualify, and only two:
	///
	/// * A literal — already a constant in the plan, so reading it needs no environment at all.
	/// * `$session` — read out of the very context this plan will run in, so the value folded in is
	///   the one the evaluator would have read. Every route that binds the name refuses it: `LET`,
	///   `UNSET`, `db.set` and RPC `set` because it is in `PROTECTED_PARAM_NAMES`, and the
	///   constructs that bind a user-chosen name (a `FOR` loop parameter, a closure parameter, a
	///   user-defined function argument) because they refuse a protected name too
	///   (GHSA-ffvm-mqh3-7986). The name is therefore never rebound, so the folded value is always
	///   the session's.
	///
	/// Every other parameter is left alone. A `DEFINE PARAM` carries permissions
	/// that belong at runtime, and an ordinary binding may be rebound by an
	/// enclosing construct after the plan is built.
	///
	/// SECURITY: folding `$session` is sound because no construct can shadow it:
	/// a body that bound its own `$session` or `$auth` would otherwise read its
	/// binding where the fold read the ambient session, so the names are refused
	/// where they would be bound. The `param_is_shadowed` guard below is kept as
	/// a belt-and-suspenders check and never fires for a protected name.
	///
	/// SECURITY: folding bakes one session's values into a physical plan, which
	/// is sound only because a plan belongs to the session it was built for. A
	/// plan cache has to either key on the folded session values or decline to
	/// cache a plan that carries them; sharing one across sessions would hand
	/// the first session's `$session` to the second.
	pub(super) fn constant_root_value(&self, root: &LogicalExpr) -> Option<Value> {
		match root {
			LogicalExpr::Literal(_) | LogicalExpr::Array(_) | LogicalExpr::Object(_) => {
				super::util::try_literal_to_value(root)
			}
			// Resolved from the ambient planning context. `None` when no
			// session is attached, or when an enclosing `FOR`, closure or
			// user-defined function binds the name, in which case nothing
			// folds.
			LogicalExpr::Param(name)
				if name.as_str() == "session" && !self.param_is_shadowed("session") =>
			{
				self.ctx.value("session").cloned()
			}
			LogicalExpr::Set(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Field(_)
			| LogicalExpr::Doc
			| LogicalExpr::Current
			| LogicalExpr::Access {
				..
			}
			| LogicalExpr::Index {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Each(_)
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Filter {
				..
			}
			| LogicalExpr::Method {
				..
			}
			| LogicalExpr::Destructure {
				..
			}
			| LogicalExpr::Optional {
				..
			}
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::Broadcast {
				..
			}
			| LogicalExpr::SetField {
				..
			}
			| LogicalExpr::Prefix {
				..
			}
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Binary {
				..
			}
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => None,
		}
	}
}

/// The root and field names of a path that is a chain of plain field accesses
/// (`root.a.b`), in application order; `None` for a bare root or for a chain
/// holding any other step.
pub(super) fn constant_access_chain(expr: &LogicalExpr) -> Option<(&LogicalExpr, Vec<&str>)> {
	let mut fields = Vec::new();
	let mut current = expr;
	while let LogicalExpr::Access {
		base,
		field,
	} = current
	{
		fields.push(field.as_str());
		current = base;
	}
	if fields.is_empty() {
		return None;
	}
	fields.reverse();
	Some((current, fields))
}

/// Walk a chain of plain field accesses over constant objects.
///
/// Deliberately narrow: every value stepped through must be a `Value::Object`.
/// Requiring an object at each step is what preserves record-link semantics —
/// `$auth.email` starts at a `Value::RecordId`, whose field access is a *fetch
/// of the stored row at runtime*, so it never folds.
///
/// A missing key folds to `NONE`, which is what the field access evaluates to,
/// but only as the last step: anything after it operates on NONE and has its
/// own behaviour there (`{}.prop.is_bool()` is `false`, not NONE), so the rest
/// is left to the evaluator.
///
/// Not guarded by `substitution_is_lossy`: that predicate exists for the
/// params pass, which substitutes a *literal* into the plan, and `Value::Set`'s
/// literal form is an array literal. Nothing here goes through SQL text — the
/// folded value is handed back verbatim — so a set folds as a set, which is
/// the payload shape a `CONTEXT` clause returning a set produces.
pub(super) fn fold_constant_field_chain(root: &Value, fields: &[&str]) -> Option<Value> {
	let mut current = root;
	for (i, name) in fields.iter().enumerate() {
		let Value::Object(object) = current else {
			return None;
		};
		match object.get(name) {
			Some(next) => current = next,
			None if i + 1 == fields.len() => return Some(Value::None),
			None => return None,
		}
	}
	Some(current.clone())
}

#[cfg(test)]
mod fold_tests {
	use super::*;
	use crate::val::{Object, RecordId, Value};

	fn object(pairs: Vec<(&str, Value)>) -> Value {
		let mut obj = Object::default();
		for (k, v) in pairs {
			obj.insert(k, v);
		}
		Value::Object(obj)
	}

	fn parse_path(src: &str) -> LogicalExpr {
		crate::exec::operators::test_util::parse_expr(src)
	}

	#[test]
	fn an_access_chain_is_read_root_first() {
		let path = parse_path("$session.data.org");
		let (root, fields) = constant_access_chain(&path).expect("a chain");
		assert!(matches!(root, LogicalExpr::Param(name) if name.as_str() == "session"));
		assert_eq!(fields, vec!["data", "org"]);
		assert!(constant_access_chain(&parse_path("$session")).is_none());
		assert!(constant_access_chain(&parse_path("$session.data[0]")).is_none());
		assert!(constant_access_chain(&parse_path("$session.data.*")).is_none());
	}

	#[test]
	fn folds_a_chain_of_object_fields() {
		let root = object(vec![("data", object(vec![("org", Value::from("acme"))]))]);
		assert_eq!(fold_constant_field_chain(&root, &["data", "org"]), Some(Value::from("acme")));
	}

	#[test]
	fn a_missing_key_folds_to_none_only_as_the_last_step() {
		let root = object(vec![("data", Value::None)]);
		assert_eq!(fold_constant_field_chain(&root, &["absent"]), Some(Value::None));
		assert_eq!(fold_constant_field_chain(&root, &["absent", "deeper"]), None);
	}

	/// The stopping rule that preserves record-link semantics: `$auth.email`
	/// must stay a runtime fetch of the stored row, never a plan-time pick.
	#[test]
	fn refuses_to_step_through_a_record_link() {
		let rid = Value::RecordId(RecordId::new(crate::val::TableName::new("user"), 1));
		let root = object(vec![("rd", rid.clone())]);
		assert_eq!(fold_constant_field_chain(&root, &["rd", "email"]), None);
		assert_eq!(fold_constant_field_chain(&rid, &["email"]), None);
	}

	/// A set folds, and folds as a set: nothing here goes through SQL text.
	#[test]
	fn folds_a_set_without_flattening_it_to_an_array() {
		let mut set = crate::val::Set::new();
		set.insert(Value::from("acme"));
		set.insert(Value::from("globex"));
		let folded =
			fold_constant_field_chain(&object(vec![("orgs", Value::Set(set.clone()))]), &["orgs"]);
		assert_eq!(folded, Some(Value::Set(set.clone())));
		assert!(!matches!(folded, Some(Value::Array(_))), "a set must not fold to an array");

		let root = object(vec![(
			"data",
			object(vec![("orgs", Value::Set(set.clone())), ("name", Value::from("alice"))]),
		)]);
		assert_eq!(fold_constant_field_chain(&root, &["data", "name"]), Some(Value::from("alice")));
		assert_eq!(
			fold_constant_field_chain(&root, &["data"]),
			Some(object(vec![("orgs", Value::Set(set)), ("name", Value::from("alice"))]))
		);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::parse_expr;

	#[test]
	fn repeat_recurse_is_found_along_the_path_and_in_alias_bodies_only() {
		let body = |src: &str| {
			let expr = parse_expr(src);
			let LogicalExpr::Recurse(recurse) = expr else {
				panic!("expected a recursion, got {expr:?}");
			};
			recurse.path.expect("a body").body().clone()
		};
		assert!(contains_repeat_recurse(&body("a.{..}.b.@")));
		assert!(contains_repeat_recurse(&body("a.{..}.{ id, kids: b.@ }")));
		assert!(contains_repeat_recurse(&body("a.{..}(.b.@)")));
		assert!(!contains_repeat_recurse(&body("a.{..}.b")));
		// An expression a step holds beside its base is not on the path.
		assert!(!contains_repeat_recurse(&body("a.{..}.b[WHERE c = (d.@)]")));
	}
}
