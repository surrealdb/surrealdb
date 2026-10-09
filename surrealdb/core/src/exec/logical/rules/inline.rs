//! A call to a `DEFINE FUNCTION` whose body is one expression is replaced by
//! that expression.
//!
//! The functions a call may be replaced by are decided once per statement,
//! with the caller in hand, by [`super::super::functions::Functions`]: their
//! `PERMISSIONS` are `FULL`, their `AUTH LIMIT` does not narrow the caller,
//! the caller may call them, and their body is a single expression over the
//! arguments alone. This rule decides the rest at the call site, so that the
//! replacement evaluates to what the call would have:
//!
//! - The call supplies every argument, or every argument it leaves out is one whose kind admits
//!   `NONE`; an argument left out is bound to `NONE`, as the call would leave it unbound.
//! - Every argument is pure ([`super::is_pure`]): the body may name a parameter several times or
//!   not at all, and the substituted expression is evaluated once per mention.
//! - No argument can raise ([`super::may_raise`]) — a literal, a field read or a comparison over
//!   those qualifies; a cast, arithmetic or an index step keeps the call. A call evaluates and
//!   coerces every argument before its body, where the substituted form evaluates each where its
//!   parameter sits: nothing at all where the body never names it, after part of the body where it
//!   does, and not at all on a path that skips it — an `IF` branch, a `??`, a `[WHERE]` predicate.
//!   Naming the parameter is therefore not evidence that the argument is still evaluated, so an
//!   argument that can raise declines the call and the error the call would report is reported.
//! - Every argument's kind, against the row the call is evaluated on, is one the declared kind
//!   coerces by identity ([`coercion_is_identity`]): the call coerces each argument to its declared
//!   kind, and a coercion that could convert or fail is not elided. The result's kind, likewise, is
//!   one the declared return kind coerces by identity. An argument's kind is the *checked* one
//!   ([`LogicalExpr::checked_kind`]), as the `kind` rule reads it, and an unproven kind is read as
//!   `any`: a stored field's declaration is enforced on write alone, so a value written before the
//!   declaration — or before `DEFINE FIELD OVERWRITE` retyped the field — holds what it held, and
//!   the coercion the call owes it is the one that reports the mismatch. A parameter declared `any`
//!   coerces every value by identity and inlines whatever the argument is. The result's kind is
//!   inferred ([`LogicalExpr::kind`]) rather than read off a declaration, which is sound because
//!   every argument has already been proven to hold its declared kind and the body is an expression
//!   over the arguments alone.
//! - An argument that reads the row — a field, the document, the current value, a row parameter, a
//!   lookup, a call evaluated against the row — is substituted only where the body evaluates its
//!   parameter against the same value as the call: not inside a broadcast body, a `[WHERE]`
//!   predicate, an optional's continuation, a recursion, a destructure alias, a lookup's clauses or
//!   a closure, where a field read means the element rather than the row
//!   ([`super::visit_positions`]). An argument that reads nothing of the row may stand anywhere.
//!
//! The rule rewrites the scalars of every operator against the schema of the
//! row they are evaluated on ([`super::rewrite_row_positions`]), and inside a body over
//! another value against the open schema, where only an argument declared
//! `any` qualifies. A `MATCH` plan's scalars are left as they are.

use super::{
	RuleContext, RuleDef, is_pure, is_row_free, may_raise, rewrite_positions, visit_positions,
};
use crate::expr::logical::{
	LogicalExpr, LogicalLiteral, LogicalOp, OpKind, Schema, coercion_is_identity,
};
use crate::expr::{Function, Kind};

pub(super) const RULE: RuleDef = RuleDef {
	name: "inline",
	// A call to inline can sit in any node's scalars.
	kinds: OpKind::ALL,
	apply,
};

fn apply(node: &mut LogicalOp, cx: &mut RuleContext<'_>) -> bool {
	if cx.functions.is_empty() {
		return false;
	}
	super::rewrite_row_positions(node, |e| is_candidate(e, cx), |e, schema| inlined(e, schema, cx))
}

/// Whether `expr` is a call this rule could replace: one to a `DEFINE
/// FUNCTION` the caller may inline. Whether the call site itself qualifies is
/// [`inlined`]'s question; this one only says whether to ask it.
fn is_candidate(expr: &LogicalExpr, cx: &RuleContext<'_>) -> bool {
	let LogicalExpr::Call(call) = expr else {
		return false;
	};
	let Function::Custom(name) = &call.receiver else {
		return false;
	};
	cx.functions.inlinable(name).is_some()
}

/// The body of the function `expr` calls with the arguments substituted, when
/// `expr` is such a call and the site qualifies; see the module documentation.
fn inlined(expr: &LogicalExpr, schema: &Schema, cx: &RuleContext<'_>) -> Option<LogicalExpr> {
	let LogicalExpr::Call(call) = expr else {
		return None;
	};
	let Function::Custom(name) = &call.receiver else {
		return None;
	};
	let body = cx.functions.inlinable(name)?;
	if call.args.len() > body.args.len() {
		return None;
	}
	let mut bindings: Vec<(&str, LogicalExpr)> = Vec::with_capacity(body.args.len());
	for (i, (param, declared)) in body.args.iter().enumerate() {
		let arg = match call.args.get(i) {
			Some(arg) => arg.clone(),
			None => {
				if !declared.can_be_none() {
					return None;
				}
				LogicalExpr::Literal(LogicalLiteral::None)
			}
		};
		let actual = arg.checked_kind(schema).unwrap_or(Kind::Any);
		if !is_pure(&arg, cx.registry) || !coercion_is_identity(declared, &actual) {
			return None;
		}
		// A call evaluates every argument before its body; the inlined form
		// evaluates each where its parameter sits. Keeping the two the same
		// for an argument that can raise would take proving both that the
		// body evaluates the parameter on every path — a mention is not
		// that, since an `IF` branch, a `??` or a `[WHERE]` predicate may
		// skip it — and that nothing evaluated before it raises first.
		// Neither is proved here, so an argument that can raise declines the
		// call.
		if may_raise(&arg, cx.permission_checked_params) {
			return None;
		}
		bindings.push((param.as_str(), arg));
	}
	// A row-reading argument may stand only where its parameter is evaluated
	// against the call's own value.
	let placeable = visit_positions(&body.expr, true, &mut |e, at_row| {
		if let LogicalExpr::Param(param) = e
			&& !at_row
			&& let Some((_, arg)) = bindings.iter().find(|(name, _)| *name == param.as_str())
			&& !is_row_free(arg, cx.registry)
		{
			return std::ops::ControlFlow::Break(());
		}
		std::ops::ControlFlow::Continue(())
	})
	.is_continue();
	if !placeable {
		return None;
	}
	let mut out = body.expr.clone();
	rewrite_positions(&mut out, true, &mut |e, _| {
		if let LogicalExpr::Param(param) = e
			&& let Some((_, arg)) = bindings.iter().find(|(name, _)| *name == param.as_str())
		{
			*e = arg.clone();
		}
	});
	// The result's kind is inferred, not read off a declaration: every
	// argument has already been proven to hold its declared kind, and the body
	// is an expression over the arguments alone, so what the inference rests on
	// is checked.
	if let Some(returns) = &body.returns
		&& !coercion_is_identity(returns, &out.kind(schema))
	{
		return None;
	}
	Some(out)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::super::super::functions::{Functions, InlineBody};
	use super::super::super::lower_test as lower;
	use super::super::test_support::{resolved, rewritten_with};
	use super::*;
	use crate::expr::logical::{FieldPath, FieldSchema, lower_expr};

	/// The expression of `text`, with `$` parameters as written.
	fn expr(text: &str) -> LogicalExpr {
		lower_expr(crate::syn::expr(text).expect("expression parses")).expect("expression lowers")
	}

	/// A function to inline: its name, arguments, return kind and body text.
	type Body<'a> = (&'a str, &'a [(&'a str, Kind)], Option<Kind>, &'a str);

	fn functions(bodies: &[Body<'_>]) -> Functions {
		Functions::from_bodies(bodies.iter().map(|(name, args, returns, body)| {
			(
				(*name).to_owned(),
				InlineBody {
					args: args.iter().map(|(n, k)| ((*n).to_owned(), k.clone())).collect(),
					returns: returns.clone(),
					expr: expr(body),
				},
			)
		}))
	}

	fn typed() -> Schema {
		Schema::closed(vec![
			FieldSchema::new(FieldPath::field("id"), Kind::Record(vec![TableName::from("t")])),
			FieldSchema::new(FieldPath::field("a"), Kind::Int),
			FieldSchema::new(FieldPath::field("s"), Kind::String),
			FieldSchema::new(FieldPath::field("o"), Kind::option(Kind::Int)),
			FieldSchema::new(FieldPath::field("arr"), Kind::Array(Box::new(Kind::Int), None)),
		])
	}

	/// The same fields as columns a layer coerces to their kinds on landing,
	/// so their kinds are checked rather than merely declared.
	fn checked() -> Schema {
		Schema::closed(
			typed()
				.fields
				.into_iter()
				.map(|field| FieldSchema::checked(field.path, field.kind))
				.collect(),
		)
	}

	fn inlined(sql: &str, functions: &Functions) -> String {
		rewritten_with(resolved(lower(sql), "t", &checked()), functions).to_string()
	}

	#[test]
	fn a_call_over_a_merely_declared_field_keeps_its_coercion() {
		// A declaration is enforced on write, so a value stored before it
		// holds what it held and the coercion the call owes it stands.
		let fns = functions(&[("double", &[("x", Kind::Int)], None, "$x * 2")]);
		let plan = rewritten_with(
			resolved(lower("SELECT * FROM t WHERE fn::double(a) > 4"), "t", &typed()),
			&fns,
		)
		.to_string();
		assert_eq!(plan, "Filter[fn::double(a) > 4]\n  Source[t]");
		// A parameter declared `any` coerces every value by identity, so it
		// inlines whatever the argument's kind is known to be.
		let fns = functions(&[("id", &[("x", Kind::Any)], None, "$x")]);
		let plan = rewritten_with(
			resolved(lower("SELECT * FROM t WHERE fn::id(a) > 4"), "t", &typed()),
			&fns,
		)
		.to_string();
		assert_eq!(plan, "Filter[a > 4]\n  Source[t]");
	}

	#[test]
	fn a_call_with_typed_arguments_is_replaced_by_the_body() {
		let fns = functions(&[("double", &[("x", Kind::Int)], Some(Kind::Number), "$x * 2")]);
		let plan = inlined("SELECT * FROM t WHERE fn::double(a) > 4", &fns);
		assert_eq!(plan, "Filter[a * 2 > 4]\n  Source[t]");
		// A literal argument, and a body naming its parameter twice.
		let fns = functions(&[("square", &[("x", Kind::Int)], None, "$x * $x")]);
		let plan = inlined("SELECT fn::square(3) FROM t", &fns);
		assert!(plan.starts_with("Project[3 * 3"), "{plan}");
	}

	#[test]
	fn a_call_whose_argument_kind_would_be_converted_is_kept() {
		let fns = functions(&[("double", &[("x", Kind::Int)], None, "$x * 2")]);
		for sql in [
			// `option<int>` may be `NONE`, which `int` refuses.
			"SELECT * FROM t WHERE fn::double(o) > 4",
			// A string is not an int.
			"SELECT * FROM t WHERE fn::double(s) > 4",
			// An unresolved field is `any`.
			"SELECT * FROM u WHERE fn::double(a) > 4",
			// Too many arguments.
			"SELECT * FROM t WHERE fn::double(a, 1) > 4",
		] {
			let plan = inlined(sql, &fns);
			assert!(plan.contains("fn::double("), "`{sql}`:\n{plan}");
		}
		// Declared `any`, anything qualifies.
		let fns = functions(&[("id", &[("x", Kind::Any)], None, "$x")]);
		let plan = inlined("SELECT * FROM u WHERE fn::id(a) > 4", &fns);
		assert_eq!(plan, "Project[*]\n  Filter[a > 4]\n    Source[u]");
	}

	#[test]
	fn an_argument_that_may_raise_keeps_the_call() {
		let fns = functions(&[("one", &[("x", Kind::Any)], None, "1")]);
		// Inlined: evaluating these cannot raise, so where the substituted
		// body evaluates them, or whether it does at all, cannot be observed.
		for sql in [
			"SELECT fn::one(1) FROM t",
			"SELECT fn::one(a) FROM t",
			"SELECT fn::one(a = 1 AND s != 'x') FROM t",
			"SELECT fn::one([a, !a]) FROM t",
		] {
			let plan = inlined(sql, &fns);
			assert!(plan.starts_with("Project[1"), "`{sql}`:\n{plan}");
		}
		// Kept: a cast, arithmetic, a call or an index step may raise, and
		// this body never evaluates the argument at all.
		for sql in [
			"SELECT fn::one(<int> NONE) FROM t",
			"SELECT fn::one(a + 1) FROM t",
			"SELECT fn::one(-a) FROM t",
			"SELECT fn::one(string::len(s)) FROM t",
			"SELECT fn::one(arr[0]) FROM t",
		] {
			let plan = inlined(sql, &fns);
			assert!(plan.contains("fn::one("), "`{sql}`:\n{plan}");
		}
		// Kept although the body names the parameter: a name is not a
		// guarantee that the substituted body evaluates it, nor that nothing
		// before it raises first.
		let fns = functions(&[
			("same", &[("x", Kind::Any)], None, "$x"),
			("pick", &[("use", Kind::Bool), ("x", Kind::Any)], None, "IF $use THEN $x ELSE 1 END"),
		]);
		for sql in
			["SELECT fn::same(<int> NONE) FROM t", "SELECT fn::pick(false, <int> NONE) FROM t"]
		{
			let plan = inlined(sql, &fns);
			assert!(plan.contains("fn::"), "`{sql}`:\n{plan}");
		}
	}

	#[test]
	fn a_return_kind_that_would_convert_keeps_the_call() {
		let fns = functions(&[("text", &[("x", Kind::Int)], Some(Kind::String), "$x * 2")]);
		let plan = inlined("SELECT fn::text(a) FROM t", &fns);
		assert!(plan.contains("fn::text(a)"), "{plan}");
		let fns = functions(&[("num", &[("x", Kind::Int)], Some(Kind::Number), "$x * 2")]);
		let plan = inlined("SELECT fn::num(a) FROM t", &fns);
		assert!(!plan.contains("fn::num("), "{plan}");
	}

	#[test]
	fn an_impure_argument_keeps_the_call() {
		let fns = functions(&[("double", &[("x", Kind::Int)], None, "$x * 2")]);
		for sql in [
			"SELECT fn::double(rand::int()) FROM t",
			"SELECT fn::double(fn::other(a)) FROM t",
			"SELECT fn::double((SELECT VALUE a FROM t)[0]) FROM t",
		] {
			let plan = inlined(sql, &fns);
			assert!(plan.contains("fn::double("), "`{sql}`:\n{plan}");
		}
	}

	#[test]
	fn a_missing_optional_argument_is_none_and_a_missing_required_one_keeps_the_call() {
		let fns = functions(&[(
			"pad",
			&[("x", Kind::Int), ("y", Kind::option(Kind::Int))],
			None,
			"[$x, $y]",
		)]);
		let plan = inlined("SELECT fn::pad(a) FROM t", &fns);
		assert!(plan.starts_with("Project[[a, NONE]"), "{plan}");
		let plan = inlined("SELECT fn::pad() FROM t", &fns);
		assert!(plan.contains("fn::pad()"), "{plan}");
	}

	#[test]
	fn a_row_reading_argument_is_not_placed_inside_a_body() {
		// `$x` stands inside the `[WHERE]` body, where a field read means the
		// element: the field argument cannot go there, a literal can.
		let fns = functions(&[("has", &[("x", Kind::Any)], None, "$this.arr[WHERE $x = 1]")]);
		let fns_any = functions(&[("pick", &[("x", Kind::Any)], None, "[1, 2][WHERE $x > 0]")]);
		drop(fns);
		let plan = inlined("SELECT fn::pick(a) FROM t", &fns_any);
		assert!(plan.contains("fn::pick(a)"), "{plan}");
		let plan = inlined("SELECT fn::pick(3) FROM t", &fns_any);
		assert!(!plan.contains("fn::pick("), "{plan}");
	}

	#[test]
	fn a_call_inside_a_body_sees_the_open_schema() {
		// Inside `arr[WHERE …]` the field `a` is the element's, not the row's
		// declared `int`: only an `any` parameter qualifies there.
		let int = functions(&[("double", &[("x", Kind::Int)], None, "$x * 2")]);
		let plan = inlined("SELECT arr[WHERE fn::double(a) > 1] FROM t", &int);
		assert!(plan.contains("fn::double(a)"), "{plan}");
		let any = functions(&[("double", &[("x", Kind::Any)], None, "$x * 2")]);
		let plan = inlined("SELECT arr[WHERE fn::double(a) > 1] FROM t", &any);
		assert!(!plan.contains("fn::double("), "{plan}");
	}
}
