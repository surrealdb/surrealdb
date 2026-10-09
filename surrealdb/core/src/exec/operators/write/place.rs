//! A place: the address a data clause writes to, compiled.
//!
//! `SET a.b = 1`, `SET tags[WHERE t = 'x'].n += 1`, `SET a[$i] = 2`,
//! `UNSET a.b` — the left-hand side of each is an [`Idiom`], walked into the
//! record and mutated where it lands. A place is not an expression: an
//! expression produces a value, a place names where a value goes, so the
//! [`PlaceStep`]s a path compiles to are what the mutators walk, and
//! `Planner::physical_path` — which composes a path into something that
//! *reads* — cannot stand in for them.
//!
//! What a place shares with a read path is the expressions a path embeds:
//! `[WHERE …]` carries a predicate, `[expr]` a subscript. Those are compiled
//! once, when the statement is planned, into [`PhysicalExpr`]s. The write
//! operator has no other evaluator, so an expression left uncompiled inside a
//! place could not be evaluated at all when the place is walked.
//!
//! # A place also reads
//!
//! A compound assignment — `+=`, `-=`, `+?` — combines its right-hand side
//! with what the place already holds, so the address has to answer a read as
//! well as a write. That read walks these same steps
//! ([`read`](super::apply::read)) rather than compiling the path as an
//! expression: an expression read resolves what a `[…]` subscript embeds
//! against the record, where [`subscript`] resolves it with no record in
//! scope. Walking the steps is what makes `SET a[b] += 1` combine with the
//! value at the key it writes to rather than with the value at the record's
//! `b` field.
//!
//! # Which parts a place can address
//!
//! A place addresses the parts the record mutators descend through, and only
//! those. Each is one [`PlaceStep`]:
//!
//! | Part | As a place |
//! |------|------------|
//! | `Field(name)` | A constant object key. |
//! | `Lookup(l)` | A constant object key, spelled as the lookup renders. |
//! | `Value(expr)` | An object key or an array index, from a per-record subscript. |
//! | `Where(pred)` | The elements of an array the predicate accepts. |
//! | `First` / `Last` | The first or last element of an array. |
//! | `All` | Every element of an array, every value of an object. |
//!
//! Every other part — `Flatten`, `Start`, `Method`, `Destructure`, `Optional`,
//! `Recurse`, `Doc` and `RepeatRecurse` — names no address, and compiles to
//! [`PlaceStep::Unaddressed`]. A walk that reaches it has landed on something it
//! does not address, so the assignment writes nothing and a removal removes
//! nothing; the steps before it are walked as usual, and the steps after it are
//! never reached. Dropping such a part instead would widen the place to its
//! prefix and write over a part of the record the statement never named.
//!
//! # The steps are an address, not a walk
//!
//! Nothing here descends into a record. The three walks over these steps
//! disagree at every one of them — an assignment creates the objects an absent
//! path needs and writes at the end of it, a removal stops where the path stops
//! and removes what it lands on, a read yields what is there and `NONE` where
//! nothing is — so a shared walk would be parameterised by the difference at
//! each step until it was three walks in one body. What they do share is this
//! address, and the frames its embedded expressions are evaluated in:
//! [`subscript`] and [`accepts`].

use std::sync::Arc;

use surrealdb_types::ToSql;

use crate::err::{EngineError, Error};
use crate::exe::FlowResultExt as _;
use crate::exec::planner::Planner;
use crate::exec::{BoxFut, EvalContext, ExecutionContext, PhysicalExpr};
use crate::expr::logical::lower_expr;
use crate::expr::{ControlFlow, Expr, Idiom, Part};
use crate::val::Value;

/// One step of a place: one part of the path, with whatever it embeds
/// compiled.
///
/// `E` is how an embedded expression is held: the operator tree holds the
/// compiled [`PhysicalExpr`], and a lowered program holds the index of the
/// pipeline that computes it. The mutators walk either through the
/// [`PlaceEval`] that knows how to evaluate that form.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PlaceStep<E = Arc<dyn PhysicalExpr>> {
	/// `.name` — an object key fixed by the statement.
	Field(String),
	/// `->edge`, `<-edge` and their kin. A place addresses a lookup as the
	/// constant object key it renders to, never as a graph traversal: the
	/// record being written is a value, and a lookup written into one of its
	/// paths is a key like any other.
	Lookup(String),
	/// `[expr]` — an object key or an array index, evaluated per record by
	/// [`subscript`]. Which of the two it is depends on what the step lands on
	/// and on the value the subscript produces, so it is one step rather than
	/// two.
	Subscript {
		/// The subscript, evaluated per record.
		expr: E,
		/// Whether the statement wrote a literal number here rather than an
		/// expression. A record mutator reads it only to decide how a step
		/// *following* a [`PlaceStep::Where`] counts, which is the one place
		/// the two disagree: an assignment takes the special reading for a
		/// literal index alone, a removal for any subscript.
		literal_index: bool,
	},
	/// `[WHERE …]` — the elements of an array the predicate accepts, judged
	/// one element at a time by [`accepts`].
	///
	/// The step selects a *set* of elements, so what follows it applies to each
	/// of them, and an index that follows it counts within the selection rather
	/// than within the array: `tags[WHERE n > 1][0]` is the first accepted
	/// element, not element zero.
	Where(E),
	/// The first element of an array, which a path renders as `[0]`. Written
	/// text never produces it: `a[0]` parses as a literal subscript and
	/// compiles to [`PlaceStep::Subscript`], so this step reaches a place only
	/// from a path built without the parser.
	First,
	/// `[$]` — the last element of an array.
	Last,
	/// `[*]` — every element of an array, and every value of an object. As with
	/// [`PlaceStep::Where`] it selects a set, and what follows applies to each
	/// member.
	All,
	/// A part that names no address — a method call, a destructure, a
	/// recursion. A walk that reaches it lands on something it does not
	/// address and stops there: nothing is written, nothing is removed, and a
	/// read finds `NONE`.
	Unaddressed,
}

/// The address a data clause writes to, with its expressions compiled.
///
/// The path is kept beside the steps because the raw form is what the
/// statement's own checks read: `ComputedData::pick` finds an assignment by the
/// `Idiom` it was written with, and the special-field check standing on it
/// compares that path. Those ask what was *named*; the steps are how it is
/// *reached*.
#[derive(Debug)]
pub(crate) struct WritePlace {
	path: Idiom,
	steps: Vec<PlaceStep>,
}

impl WritePlace {
	/// Compile the place an [`Idiom`] names.
	///
	/// Every expression the path embeds is compiled here, once for the
	/// statement: a place is walked once per record, and compiling it per
	/// record would re-plan a user expression per row. A part that names no
	/// address compiles to [`PlaceStep::Unaddressed`] without compiling
	/// anything it embeds, since the walk never reaches those expressions.
	///
	/// Fails only on a tree no parser builds, where an embedded expression has
	/// no logical form.
	pub(crate) async fn compile(planner: &Planner<'_>, path: Idiom) -> Result<Self, Error> {
		let mut steps = Vec::with_capacity(path.0.len());
		for part in &path.0 {
			steps.push(match part {
				Part::Field(name) => PlaceStep::Field(name.as_str().to_owned()),
				Part::Lookup(lookup) => PlaceStep::Lookup(lookup.to_sql()),
				Part::Value(expr) => PlaceStep::Subscript {
					expr: compile_embedded(planner, expr).await?,
					literal_index: part.as_old_index().is_some(),
				},
				Part::Where(pred) => PlaceStep::Where(compile_embedded(planner, pred).await?),
				Part::First => PlaceStep::First,
				Part::Last => PlaceStep::Last,
				Part::All => PlaceStep::All,
				Part::Flatten
				| Part::Start(_)
				| Part::Method(_, _)
				| Part::Destructure(_)
				| Part::Optional
				| Part::Recurse(_, _, _)
				| Part::Doc
				| Part::RepeatRecurse => PlaceStep::Unaddressed,
			});
		}
		Ok(Self {
			path,
			steps,
		})
	}

	/// The path as the statement wrote it.
	pub(crate) fn path(&self) -> &Idiom {
		&self.path
	}

	/// The compiled steps, in the order they are walked.
	pub(crate) fn steps(&self) -> &[PlaceStep] {
		&self.steps
	}

	/// Whether the place names no step at all, which addresses the record
	/// itself rather than anything inside it.
	pub(crate) fn is_empty(&self) -> bool {
		self.steps.is_empty()
	}
}

/// Compile one expression a path embeds.
///
/// The statement holds its clauses in the expression layer's form, so the
/// scalar is lowered here and compiled by the planner compiling the statement —
/// the same route, and so the same principal, transaction and re-entry depth,
/// as the right-hand side it is written beside.
async fn compile_embedded(
	planner: &Planner<'_>,
	expr: &Expr,
) -> Result<Arc<dyn PhysicalExpr>, Error> {
	let logical = lower_expr(expr.clone()).map_err(|why| {
		Error::from(EngineError::unreachable(format!(
			"a place cannot hold {}: {why}",
			expr.to_sql()
		)))
	})?;
	planner.physical_expr(logical).await
}

/// Evaluate a [`PlaceStep::Subscript`] for one record.
///
/// The subscript is evaluated with **no record in scope**: `a[b]` names the key
/// `b` resolves to in the statement's own scope, not the record's `b` field,
/// and a place sees none of the bindings the data clause's right-hand sides are
/// evaluated under. It is evaluated where the place is walked rather than where
/// the clause was evaluated, so an order that applies one place twice evaluates
/// its subscripts twice.
///
/// A `RETURN` inside it is the subscript's value; `BREAK` and `CONTINUE` have
/// nothing here to leave and fail the statement.
pub(crate) async fn subscript(
	expr: &Arc<dyn PhysicalExpr>,
	exec: &ExecutionContext,
) -> Result<Value, ControlFlow> {
	expr.evaluate(EvalContext::from_exec_ctx(exec)).await.catch_return().map_err(ControlFlow::Err)
}

/// Judge a [`PlaceStep::Where`] predicate against one element.
///
/// The element is the predicate's current record, so `tags[WHERE n > 1]` reads
/// `n` off the element rather than off the record being written. Control flow
/// is treated as in [`subscript`].
pub(crate) async fn accepts(
	pred: &Arc<dyn PhysicalExpr>,
	element: &Value,
	exec: &ExecutionContext,
) -> Result<bool, ControlFlow> {
	let eval = EvalContext::from_exec_ctx(exec);
	Ok(pred
		.evaluate(eval.with_value_and_doc(element))
		.await
		.catch_return()
		.map_err(ControlFlow::Err)?
		.is_truthy())
}

/// How the mutators evaluate what a place embeds, for one form of
/// [`PlaceStep`]. Both questions are answered with no record in scope but the
/// element a `[WHERE …]` judges, and a `RETURN` inside the expression is its
/// value.
pub(crate) trait PlaceEval: Send {
	/// How an embedded expression is held in the steps this evaluates.
	type Expr: Send + Sync;

	/// The value of a [`PlaceStep::Subscript`]'s expression.
	fn subscript<'a>(&'a mut self, expr: &'a Self::Expr) -> BoxFut<'a, Result<Value, ControlFlow>>;

	/// Whether a [`PlaceStep::Where`]'s predicate accepts `element`.
	fn accepts<'a>(
		&'a mut self,
		expr: &'a Self::Expr,
		element: &'a Value,
	) -> BoxFut<'a, Result<bool, ControlFlow>>;
}

/// Evaluates compiled expressions in an execution context, as [`subscript`]
/// and [`accepts`] define.
pub(crate) struct TreeEval<'e>(pub(crate) &'e ExecutionContext);

impl PlaceEval for TreeEval<'_> {
	type Expr = Arc<dyn PhysicalExpr>;

	fn subscript<'a>(&'a mut self, expr: &'a Self::Expr) -> BoxFut<'a, Result<Value, ControlFlow>> {
		Box::pin(subscript(expr, self.0))
	}

	fn accepts<'a>(
		&'a mut self,
		expr: &'a Self::Expr,
		element: &'a Value,
	) -> BoxFut<'a, Result<bool, ControlFlow>> {
		Box::pin(accepts(expr, element, self.0))
	}
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{field_idiom, root_ctx, val};

	/// Compile the place `src` names, or report why it was refused.
	async fn compile(src: &str) -> Result<WritePlace, String> {
		let ctx = root_ctx();
		let planner = Planner::new(ctx.ctx(), ctx.function_registry());
		WritePlace::compile(&planner, field_idiom(src)).await.map_err(|err| err.to_string())
	}

	/// The kinds a place compiled to, as names, so a test states the shape it
	/// expects rather than matching a tree.
	fn shape(place: &WritePlace) -> Vec<&'static str> {
		place
			.steps()
			.iter()
			.map(|step| match step {
				PlaceStep::Field(_) => "field",
				PlaceStep::Lookup(_) => "lookup",
				PlaceStep::Subscript {
					..
				} => "subscript",
				PlaceStep::Where(_) => "where",
				PlaceStep::First => "first",
				PlaceStep::Last => "last",
				PlaceStep::All => "all",
				PlaceStep::Unaddressed => "unaddressed",
			})
			.collect()
	}

	/// A constant path compiles to constant steps, and keeps the path the
	/// statement wrote: the raw form is what the statement's own checks match
	/// an assignment by.
	#[tokio::test]
	async fn a_constant_path_compiles_to_constant_steps() {
		let place = compile("a.b").await.unwrap();
		assert_eq!(shape(&place), ["field", "field"]);
		assert_eq!(place.path(), &field_idiom("a.b"));
		assert!(!place.is_empty());
		let PlaceStep::Field(name) = &place.steps()[1] else {
			panic!("expected a field step, got {:?}", place.steps()[1]);
		};
		assert_eq!(name, "b");
	}

	/// Each addressable part compiles to its own kind, so a walk never has to
	/// re-read the path to tell an index from a filter.
	#[tokio::test]
	async fn every_addressable_part_compiles_to_its_own_step() {
		assert_eq!(shape(&compile("a[*]").await.unwrap()), ["field", "all"]);
		assert_eq!(shape(&compile("a[$]").await.unwrap()), ["field", "last"]);
		assert_eq!(shape(&compile("a[0]").await.unwrap()), ["field", "subscript"]);
		assert_eq!(shape(&compile("a[$i]").await.unwrap()), ["field", "subscript"]);
		assert_eq!(shape(&compile("a[WHERE n > 1].b").await.unwrap()), ["field", "where", "field"]);
	}

	/// A part that addresses nothing compiles to a step that stops the walk
	/// where it stands, rather than being dropped: dropping it would widen the
	/// place to its prefix and overwrite what the statement never named.
	#[tokio::test]
	async fn a_part_that_addresses_nothing_stops_the_walk() {
		assert_eq!(
			shape(&compile("a.b.{ c, d }").await.unwrap()),
			["field", "field", "unaddressed"]
		);
		assert_eq!(shape(&compile("a.b.len()").await.unwrap()), ["field", "field", "unaddressed"]);
		assert_eq!(shape(&compile("a.b.@").await.unwrap()), ["field", "field", "unaddressed"]);
	}

	/// A subscript is evaluated in the statement's scope with no record in it,
	/// which is what makes `a[b]` the key `b` names rather than the record's
	/// `b` field.
	#[tokio::test]
	async fn a_subscript_is_evaluated_with_no_record_in_scope() {
		let ctx = root_ctx();
		let place = compile("a[b]").await.unwrap();
		let PlaceStep::Subscript {
			expr,
			..
		} = &place.steps()[1]
		else {
			panic!("expected a subscript step, got {:?}", place.steps()[1]);
		};
		assert_eq!(subscript(expr, &ctx).await.unwrap(), Value::None);
	}

	/// A `WHERE` step judges one element, reading its fields off that element.
	#[tokio::test]
	async fn a_where_step_judges_one_element() {
		let ctx = root_ctx();
		let place = compile("a[WHERE n > 1]").await.unwrap();
		let PlaceStep::Where(pred) = &place.steps()[1] else {
			panic!("expected a where step, got {:?}", place.steps()[1]);
		};
		assert!(accepts(pred, &val("{ n: 2 }").await, &ctx).await.unwrap());
		assert!(!accepts(pred, &val("{ n: 1 }").await, &ctx).await.unwrap());
		assert!(!accepts(pred, &val("{ }").await, &ctx).await.unwrap());
	}
}
