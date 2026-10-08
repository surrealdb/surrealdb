//! The physical form of a path: a run of steps over a root, the
//! array-continuation broadcast, and the current value.
//!
//! A logical path is a nest of step forms (`Access`, `Index`, `Filter`,
//! `Lookup`, …) over a root: the row's field, the row itself (`Current`), or
//! the expression a `(e).rest` path starts from. The planner compiles that
//! nest into one [`Step`], which holds the root and the chain of part
//! evaluators from `crate::exec::parts` in application order. Each part reads
//! its input from `ctx.current_value`, so evaluation threads the value
//! through the chain with `ctx.with_value`.
//!
//! A [`Broadcast`] is the one node with sequence semantics of its own: when
//! its base is an array, its body is applied to every element; otherwise the
//! body is applied to the base. A [`Current`] reads the value a body is
//! applied to. A [`FoldedPath`] is a path the planner resolved to its value
//! at plan time.
//!
//! # Rendering
//!
//! Every node carries the rendering of the logical expression it was
//! compiled from, so `EXPLAIN` prints a path as the idiom it was written as,
//! byte for byte, wherever the expression sits in a plan. The parts' own
//! `ToSql` implementations are not consulted.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::parts::{AllPart, LookupPart};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, CombineAccessModes, ContextLevel, ExecOperator};
use crate::expr::FlowResult;
use crate::val::Value;

// ============================================================================
// Current
// ============================================================================

/// The value a body is applied to: `ctx.current_value`, or `NONE` when the
/// context carries none.
///
/// The root of every broadcast, optional and recursion body, and of a
/// destructure alias; also a path in its own right for a trailing `.?`,
/// whose body is the value itself. Renders as nothing.
#[derive(Debug, Clone)]
pub struct Current;

impl PhysicalExpr for Current {
	fn name(&self) -> &'static str {
		"Current"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move { Ok(ctx.current_value.cloned().unwrap_or(Value::None)) })
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for Current {
	fn fmt_sql(&self, _f: &mut String, _fmt: SqlFormat) {}
}

// ============================================================================
// FoldedPath
// ============================================================================

/// A path resolved to its value at plan time: a chain of plain field accesses
/// over a constant object (`$session.data.orgs`, `{ a: { b: 1 } }.a.b`),
/// picked from once by the planner rather than on every row. The planner
/// folds only where the logical fold pass could not: the scalars compiled
/// outside a bound plan, such as a stored permission body a runtime-enforcing
/// scan compiles directly.
///
/// `try_literal` exposes the value, so `SimpleBinaryOp` and the pre-decode
/// filter consume it as the constant it is, through the same `operate::*`
/// functions the unfolded operator would use. Nothing round-trips through SQL
/// text, so a `Value::Set` survives as a set.
///
/// Its own [`ToSql`] renders the path it came from rather than its value. That
/// holds only while this node is what gets printed: once `try_literal` makes
/// the `field op literal` fast path eligible, the planner keeps only the
/// `Value`, and `SimpleBinaryOp`'s `fmt_sql` prints that, so a folded
/// `$session` payload can appear in plan text. The payload is the caller's
/// own, in their own plan, but plan text for identical SQL then varies with
/// the session.
///
/// SECURITY: a fold bakes one session's values into a physical plan. That is
/// sound only because plans are never shared between sessions: there is no
/// cross-session plan cache, and a plan built under one binding of `$session`
/// is never reused under another. A plan cache, if one is ever added, must key
/// on the folded session values or exclude plans that carry them.
#[derive(Debug, Clone)]
pub struct FoldedPath {
	/// The path as written, kept for display only.
	display: String,
	value: Value,
}

impl FoldedPath {
	pub(crate) fn new(display: String, value: Value) -> Self {
		Self {
			display,
			value,
		}
	}
}

impl PhysicalExpr for FoldedPath {
	fn name(&self) -> &'static str {
		"FoldedPath"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, _ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move { Ok(self.value.clone()) })
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn try_literal(&self) -> Option<&Value> {
		Some(&self.value)
	}
}

impl ToSql for FoldedPath {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.display);
	}
}

// ============================================================================
// Step
// ============================================================================

/// A run of path steps applied to a root.
///
/// `parts` are the part evaluators in application order; each reads its
/// input from `ctx.current_value` and yields the value the next one reads.
/// `root` is the expression the first part applies to, or `None` when it
/// applies to the current value itself — a path into the row (`a.b[0]`), or
/// a body over [`Current`] (`.b[0]` inside a broadcast).
///
/// A path rooted in the current value whose one part is a field read is a
/// *simple field* ([`PhysicalExpr::try_simple_field`]), and one whose every
/// part is a field read is a *static object field path*
/// ([`Self::try_static_object_field_path`]); both are the shapes the planner
/// and the pre-decode filter inline.
#[derive(Debug, Clone)]
pub struct Step {
	/// The rendering of the logical path this was compiled from.
	display: String,
	/// What the first part applies to; `None` for the current value.
	pub(crate) root: Option<Arc<dyn PhysicalExpr>>,
	/// The parts, in application order. Never empty.
	pub(crate) parts: Vec<Arc<dyn PhysicalExpr>>,
	/// Whether evaluating the run executes a lookup plan; see
	/// [`runs_lookup_plan`].
	runs_lookup_plan: bool,
}

impl Step {
	/// A run of `parts` over `root`, rendering as `display`.
	pub(crate) fn new(
		display: String,
		root: Option<Arc<dyn PhysicalExpr>>,
		parts: Vec<Arc<dyn PhysicalExpr>>,
	) -> Self {
		debug_assert!(!parts.is_empty(), "a step holds at least one part");
		let runs_lookup_plan = root.iter().chain(parts.iter()).any(runs_lookup_plan);
		Self {
			display,
			root,
			parts,
			runs_lookup_plan,
		}
	}

	/// The expression the first part applies to; `None` for the current
	/// value.
	#[cfg(test)]
	pub(crate) fn root(&self) -> Option<&Arc<dyn PhysicalExpr>> {
		self.root.as_ref()
	}

	/// The parts, in application order.
	#[cfg(test)]
	pub(crate) fn parts(&self) -> &[Arc<dyn PhysicalExpr>] {
		&self.parts
	}

	#[cfg(test)]
	/// The field names of a path into the current value made only of field
	/// reads (`a.b.c`): the shape a byte-level filter can follow into an
	/// encoded object.
	pub(crate) fn try_static_object_field_path(&self) -> Option<Vec<String>> {
		if self.root.is_some() {
			return None;
		}
		self.parts.iter().map(|p| p.try_simple_field().map(str::to_owned)).collect()
	}

	/// Evaluate the run. `skip_leading_each` drops a leading `.*` over the
	/// current value: inside a mapped broadcast body an element that is not a
	/// record id passes through that step unchanged.
	fn evaluate_run<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		skip_leading_each: bool,
	) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let mut parts = self.parts.iter();
			let mut value = match &self.root {
				Some(root) => root.evaluate(ctx.clone()).await?,
				None => {
					let Some(first) = parts.next() else {
						return Ok(ctx.current_value.cloned().unwrap_or(Value::None));
					};
					if skip_leading_each && first.downcast_ref::<AllPart>().is_some() {
						ctx.current_value.cloned().unwrap_or(Value::None)
					} else if self.parts.len() == 1 {
						// The part reads the current value in place, so the
						// row is never cloned for a bare field read.
						return first.evaluate(ctx).await;
					} else {
						first.evaluate(ctx.clone()).await?
					}
				}
			};
			for part in parts {
				value = part.evaluate(ctx.with_value(&value)).await?;
			}
			Ok(value)
		})
	}
}

impl PhysicalExpr for Step {
	fn name(&self) -> &'static str {
		"Step"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		let root = self.root.as_ref().map_or(ContextLevel::Root, |r| r.required_context());
		let parts =
			self.parts.iter().map(|p| p.required_context()).max().unwrap_or(ContextLevel::Root);
		root.max(parts)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		self.evaluate_run(ctx, false)
	}

	/// A run that executes a lookup plan per row is CPU-bound work the
	/// cooperative default cannot overlap, so read-only batches of those
	/// spawn one runtime task per chunk of rows (bounded, results in input
	/// order). Every other run keeps the sequential default: a plain part
	/// walk is too cheap for task dispatch to pay.
	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(async move {
			#[cfg(not(target_family = "wasm"))]
			if spawning_pays(self, self.runs_lookup_plan, &ctx, values) {
				return crate::exec::fan_out::evaluate_rows_spawned(
					Arc::new(self.clone()),
					&ctx,
					values,
					crate::exec::fan_out::SpawnedDocRoot::RowValue,
				)
				.await;
			}
			let mut results = Vec::with_capacity(values.len());
			for value in values {
				results.push(self.evaluate(ctx.with_value_and_doc(value)).await?);
			}
			Ok(results)
		})
	}

	fn access_mode(&self) -> AccessMode {
		let parts = self.parts.iter().map(|p| p.access_mode()).combine_all();
		match &self.root {
			Some(root) => parts.combine(root.access_mode()),
			None => parts,
		}
	}

	fn try_simple_field(&self) -> Option<&str> {
		match (&self.root, self.parts.as_slice()) {
			(None, [part]) => part.try_simple_field(),
			_ => None,
		}
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		let mut ops = Vec::new();
		if let Some(root) = &self.root {
			ops.extend(root.embedded_operators());
		}
		for part in &self.parts {
			ops.extend(part.embedded_operators());
		}
		ops
	}
}

impl ToSql for Step {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.display);
	}
}

// ============================================================================
// Broadcast
// ============================================================================

/// The array-continuation rule: a body applied per element of an array base.
///
/// When `base` evaluates to an array, `body` is evaluated once per element
/// with the element as the current value, and the results form the array —
/// flattened one level when `flatten` is set, which is how a lookup's
/// per-element arrays join into one. When `base` is anything else, `body` is
/// evaluated once with that value as the current value, continuation
/// included.
///
/// Mapping nests: a `Broadcast` inside a mapped body maps over its own base
/// in turn, so `rows.a.b[0]` indexes each `b` rather than the array the `.b`
/// step collected. What the plain form changes is only how the body's *first*
/// step reads the element it is handed — a leading `.*` over an element that
/// is not a record id passes the element through. See [`evaluate_plain`].
#[derive(Debug, Clone)]
pub struct Broadcast {
	/// The rendering of the logical path this was compiled from.
	display: String,
	pub(crate) base: Arc<dyn PhysicalExpr>,
	pub(crate) body: Arc<dyn PhysicalExpr>,
	flatten: bool,
	/// Whether the body's first step over the current value is `.*`.
	leading_each: bool,
	/// Whether the base is the current value itself, which the broadcast
	/// then reads in place rather than evaluating into an owned copy.
	base_is_current: bool,
	/// Whether evaluating the broadcast executes a lookup plan; see
	/// [`runs_lookup_plan`].
	runs_lookup_plan: bool,
}

impl Broadcast {
	pub(crate) fn new(
		display: String,
		base: Arc<dyn PhysicalExpr>,
		body: Arc<dyn PhysicalExpr>,
		flatten: bool,
	) -> Self {
		let leading_each = has_leading_each(&body);
		let base_is_current = base.downcast_ref::<Current>().is_some();
		let runs_lookup_plan = runs_lookup_plan(&base) || runs_lookup_plan(&body);
		Self {
			display,
			base,
			body,
			flatten,
			leading_each,
			base_is_current,
			runs_lookup_plan,
		}
	}

	/// Evaluation inside a mapped body: the base runs as a plain sequence of
	/// steps, so `skip_leading_each` reaches the first of them, and the body
	/// then maps over it as it does anywhere else.
	fn evaluate_plain<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		skip_leading_each: bool,
	) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			if self.base_is_current {
				return self.map_over_ref(ctx.current_value.unwrap_or(&Value::None), ctx).await;
			}
			let base = evaluate_plain(&*self.base, ctx.clone(), skip_leading_each).await?;
			self.map_over_ref(&base, ctx).await
		})
	}

	/// The body applied to `base`: once per element when `base` is an array,
	/// and once with `base` itself otherwise.
	///
	/// The base is borrowed rather than owned, so a broadcast over the row
	/// itself — which is what a path starting with a lookup compiles to —
	/// reads it in place instead of cloning it once per row.
	fn map_over_ref<'a>(
		&'a self,
		base: &'a Value,
		ctx: EvalContext<'a>,
	) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			match base {
				Value::Array(arr) => {
					let mut results = Vec::with_capacity(arr.len());
					for elem in arr.iter() {
						let skip = self.leading_each && !matches!(elem, Value::RecordId(_));
						results
							.push(evaluate_plain(&*self.body, ctx.with_value(elem), skip).await?);
					}
					let result = Value::Array(results.into());
					Ok(if self.flatten {
						result.flatten()
					} else {
						result
					})
				}
				other => self.body.evaluate(ctx.with_value(other)).await,
			}
		})
	}
}

impl PhysicalExpr for Broadcast {
	fn name(&self) -> &'static str {
		"Broadcast"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.base.required_context().max(self.body.required_context())
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			if self.base_is_current {
				return self.map_over_ref(ctx.current_value.unwrap_or(&Value::None), ctx).await;
			}
			let base = self.base.evaluate(ctx.clone()).await?;
			self.map_over_ref(&base, ctx).await
		})
	}

	/// A broadcast whose base or body executes a lookup plan carries the same
	/// per-row scan work a [`Step`] does — a path that begins with a run of
	/// hops compiles to one of these — so its batches spawn on the same
	/// terms; see [`Step::evaluate_batch`].
	fn evaluate_batch<'a>(
		&'a self,
		ctx: EvalContext<'a>,
		values: &'a [Value],
	) -> BoxFut<'a, FlowResult<Vec<Value>>> {
		Box::pin(async move {
			#[cfg(not(target_family = "wasm"))]
			if spawning_pays(self, self.runs_lookup_plan, &ctx, values) {
				return crate::exec::fan_out::evaluate_rows_spawned(
					Arc::new(self.clone()),
					&ctx,
					values,
					crate::exec::fan_out::SpawnedDocRoot::RowValue,
				)
				.await;
			}
			let mut results = Vec::with_capacity(values.len());
			for value in values {
				results.push(self.evaluate(ctx.with_value_and_doc(value)).await?);
			}
			Ok(results)
		})
	}

	fn access_mode(&self) -> AccessMode {
		self.base.access_mode().combine(self.body.access_mode())
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		let mut ops = self.base.embedded_operators();
		ops.extend(self.body.embedded_operators());
		ops
	}
}

impl ToSql for Broadcast {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.display);
	}
}

/// Whether evaluating `expr` against one value executes a lookup plan — a
/// graph or reference traversal.
///
/// A path compiles to a nest of [`Step`] and [`Broadcast`] nodes, and a run
/// of hops can sit at any depth in it: `->knows->person` is a lookup part
/// inside the body of a broadcast over the row. Both nodes answer this from
/// a flag they compute once, so the walk stops at the first nested node
/// rather than descending the whole path on every batch.
///
/// The walk follows the path's own structure and does not enter the
/// expressions a part holds — a `[WHERE …]` predicate, an index, a method's
/// arguments. A lookup reached only through one of those keeps the
/// sequential default.
fn runs_lookup_plan(expr: &Arc<dyn PhysicalExpr>) -> bool {
	if let Some(step) = expr.downcast_ref::<Step>() {
		return step.runs_lookup_plan;
	}
	if let Some(broadcast) = expr.downcast_ref::<Broadcast>() {
		return broadcast.runs_lookup_plan;
	}
	expr.downcast_ref::<LookupPart>().is_some()
}

/// Whether a batch of `values` is worth spawning across runtime tasks.
///
/// Running a scan plan per row is CPU-bound work on an embedded backend, so
/// the rows overlap only on separate tasks; everything else is too cheap for
/// task dispatch to pay. A writing evaluation and a recursion are excluded
/// because neither survives the context reconstruction the spawned rows are
/// rebuilt from — see [`crate::exec::fan_out::evaluate_rows_spawned`].
#[cfg(not(target_family = "wasm"))]
fn spawning_pays(
	expr: &dyn PhysicalExpr,
	runs_lookup_plan: bool,
	ctx: &EvalContext<'_>,
	values: &[Value],
) -> bool {
	runs_lookup_plan
		&& values.len() >= ctx.exec_ctx.root().ctx.config.exec.fan_out_row_threshold.max(2)
		&& ctx.recursion_ctx.is_none()
		&& expr.access_mode() == AccessMode::ReadOnly
}

/// Whether the first step a body applies to the current value is `.*`,
/// looking through the base chain of a nested broadcast.
fn has_leading_each(body: &Arc<dyn PhysicalExpr>) -> bool {
	let mut current = body;
	loop {
		if let Some(broadcast) = current.downcast_ref::<Broadcast>() {
			current = &broadcast.base;
			continue;
		}
		return match current.downcast_ref::<Step>() {
			Some(step) => {
				step.root.is_none()
					&& step.parts.first().is_some_and(|p| p.downcast_ref::<AllPart>().is_some())
			}
			None => false,
		};
	}
}

/// Evaluate a path the way the body of a mapped [`Broadcast`] runs against one
/// element: a leading `.*` is skipped when `skip_leading_each` is set, and a
/// nested broadcast takes that flag onto its own base and then maps as usual.
/// Anything that is not a path node evaluates as itself.
pub(crate) fn evaluate_plain<'a>(
	expr: &'a dyn PhysicalExpr,
	ctx: EvalContext<'a>,
	skip_leading_each: bool,
) -> BoxFut<'a, FlowResult<Value>> {
	if let Some(broadcast) = expr.downcast_ref::<Broadcast>() {
		broadcast.evaluate_plain(ctx, skip_leading_each)
	} else if let Some(step) = expr.downcast_ref::<Step>() {
		step.evaluate_run(ctx, skip_leading_each)
	} else {
		expr.evaluate(ctx)
	}
}

/// The first part of type `T` in a compiled path, searching the parts of a
/// [`Step`] and the base and body of a [`Broadcast`], outermost first.
#[cfg(test)]
pub(crate) fn find_part<T: PhysicalExpr>(
	expr: &Arc<dyn PhysicalExpr>,
) -> Option<Arc<dyn PhysicalExpr>> {
	if expr.downcast_ref::<T>().is_some() {
		return Some(Arc::clone(expr));
	}
	if let Some(step) = expr.downcast_ref::<Step>() {
		return step.root.iter().chain(step.parts.iter()).find_map(|p| find_part::<T>(p));
	}
	if let Some(broadcast) = expr.downcast_ref::<Broadcast>() {
		return find_part::<T>(&broadcast.base).or_else(|| find_part::<T>(&broadcast.body));
	}
	None
}

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use super::*;
	use crate::exec::operators::test_util::{
		TestDb, eval, eval_on, parse_sql_expr, physical_expr, root_ctx, val,
	};
	use crate::exec::parts::{FieldPart, LookupPart, WherePart};
	use crate::expr::logical::LogicalExpr;

	// =========================================================================
	// Rendering: the compiled path prints as the idiom it was written as
	// =========================================================================

	#[tokio::test]
	async fn a_compiled_path_renders_as_the_idiom_it_was_written_as() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		for src in [
			"a",
			"a.b.c",
			"a.b[0].c",
			"a[WHERE x > 1]",
			"a[$]",
			"a.*",
			"a.?.b",
			"a.{ x, y: z }",
			"->knows->person",
			"a->knows[WHERE since > 1]->person",
			"a.b.{2}(.c)",
			"a.len()",
			"(1 + 2).to_string()",
			"$p.a[0]",
		] {
			let expr = physical_expr(src, &ctx).await;
			assert_eq!(expr.to_sql(), parse_sql_expr(src).to_sql(), "rendering of {src}");
		}
	}

	#[tokio::test]
	async fn a_broadcast_renders_its_whole_path_not_only_its_base() {
		let ctx = root_ctx();
		let expr = physical_expr("a.b[0]", &ctx).await;
		assert!(
			expr.downcast_ref::<Broadcast>().is_some(),
			"a mapping step before an index broadcasts"
		);
		assert_eq!(expr.to_sql(), "a.b[0]");
	}

	// =========================================================================
	// Simple-field and static-path detection
	// =========================================================================

	#[tokio::test]
	async fn a_bare_field_is_a_simple_field_and_a_field_chain_is_a_static_path() {
		let ctx = root_ctx();
		let field = physical_expr("age", &ctx).await;
		assert_eq!(field.try_simple_field(), Some("age"));

		let chain = physical_expr("a.b.c", &ctx).await;
		assert_eq!(chain.try_simple_field(), None);
		let step = chain.downcast_ref::<Step>().expect("a field chain is one step run");
		assert_eq!(
			step.try_static_object_field_path(),
			Some(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()])
		);

		// The document form reads the row's identity, which is not a column
		// of it: a consumer that fires on a simple field read must not take
		// `@` for a read of the `id` field.
		let doc = crate::exec::planner::compile_expr(
			LogicalExpr::Doc,
			ctx.ctx(),
			ctx.function_registry(),
			None,
			None,
			0,
		)
		.await
		.expect("the document form compiles");
		assert_eq!(doc.try_simple_field(), None);

		// Anything that is not a field read stops both.
		let indexed = physical_expr("a[0]", &ctx).await;
		assert_eq!(indexed.try_simple_field(), None);
		let step = indexed.downcast_ref::<Step>().expect("an index over a field is one step run");
		assert_eq!(step.try_static_object_field_path(), None);

		// A path from a starting expression is not a path into the row.
		let started = physical_expr("$p.a", &ctx).await;
		let step = started.downcast_ref::<Step>().expect("a param path is one step run");
		assert_eq!(step.try_static_object_field_path(), None);
	}

	// =========================================================================
	// Broadcast semantics
	// =========================================================================

	#[tokio::test]
	async fn a_mapping_step_over_an_array_applies_the_rest_of_the_path_per_element() {
		let ctx = root_ctx();
		let doc = val("{ rows: [{ a: [1, 2] }, { a: [3, 4] }] }").await;
		// `[0]` indexes each element's `a`, not the collected array.
		assert_eq!(eval_on("rows.a[0]", &doc, &ctx).await.unwrap(), val("[1, 3]").await);
		// Without the continuation rule this would be `[1, 2]`.
		assert_eq!(eval_on("rows.a", &doc, &ctx).await.unwrap(), val("[[1, 2], [3, 4]]").await);
	}

	#[tokio::test]
	async fn a_broadcast_nested_in_a_mapped_body_maps_in_turn() {
		let ctx = root_ctx();
		let doc = val("{ rows: [{ a: [{ b: [1, 2] }] }] }").await;
		// Per `rows` element: `.a` gives `[{ b: [1, 2] }]`, and `.b[0]` maps
		// over that in turn, indexing each element's own `b` to `1`.
		assert_eq!(eval_on("rows.a.b[0]", &doc, &ctx).await.unwrap(), val("[[1]]").await);
		// The step applies at the element's level, so a scalar `b` has no
		// element to index.
		let doc = val("{ rows: [{ a: [{ b: 1 }, { b: 2 }] }] }").await;
		assert_eq!(eval_on("rows.a.b[0]", &doc, &ctx).await.unwrap(), val("[[NONE, NONE]]").await);
	}

	#[tokio::test]
	async fn a_broadcast_over_a_non_array_evaluates_its_body_once_with_continuation() {
		let ctx = root_ctx();
		let doc = val("{ row: { a: [{ b: [1, 2] }, { b: [3] }] } }").await;
		// `row` is an object, so the body runs once on it; inside, `.a` yields
		// an array and `.b[0]` maps over that array.
		assert_eq!(eval_on("row.a.b[0]", &doc, &ctx).await.unwrap(), val("[1, 3]").await);
	}

	#[tokio::test]
	async fn a_leading_each_in_a_mapped_body_passes_non_records_through() {
		let ctx = root_ctx();
		let doc = val("{ v: [[3, 2, 1], [1, 2, 3]] }").await;
		assert_eq!(
			eval_on("v.*[0..1]", &doc, &ctx).await.unwrap(),
			val("[[3], [1]]").await,
			"`.*` on an array element keeps it whole for the slice"
		);
		// A scalar element would be `NONE` under `.*`; the pass-through keeps
		// it for the method call instead.
		let doc = val("{ v: [1, 2] }").await;
		assert_eq!(eval_on("v.*.to_string()", &doc, &ctx).await.unwrap(), val("['1', '2']").await);
	}

	#[tokio::test]
	async fn a_literal_array_start_broadcasts_over_its_elements() {
		let ctx = root_ctx();
		assert_eq!(
			eval("[{ a: [1, 2] }, { a: [3, 4] }].a[0]", &ctx).await.unwrap(),
			val("[1, 3]").await
		);
	}

	// =========================================================================
	// Lookup continuation and fusion
	// =========================================================================

	async fn graph_db() -> TestDb {
		TestDb::new(
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE knows SCHEMALESS TYPE RELATION;
			 CREATE person:tobie SET name = 'Tobie';
			 CREATE person:jaime SET name = 'Jaime';
			 CREATE person:sam SET name = 'Sam';
			 RELATE person:tobie->knows:e1->person:jaime SET since = 2020;
			 RELATE person:jaime->knows:e2->person:sam SET since = 2021;",
		)
		.await
	}

	#[tokio::test]
	async fn consecutive_lookups_fuse_into_one_lookup_part() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let expr = physical_expr("v->knows->person", &ctx).await;
		find_part::<LookupPart>(&expr).expect("a lookup part");
		assert_eq!(
			expr.embedded_operators().len(),
			1,
			"two hops fuse into one part, so the path embeds one lookup plan"
		);

		let row = val("{ v: person:tobie }").await;
		assert_eq!(
			eval_on("v->knows->person", &row, &ctx).await.unwrap(),
			val("[person:jaime]").await
		);
	}

	#[tokio::test]
	async fn a_filter_between_lookups_flattens_and_continues_per_element() {
		let db = graph_db().await;
		let ctx = db.exec_ctx().await;
		let out = eval("person:tobie->knows->person[WHERE name = 'Jaime']->knows->person", &ctx)
			.await
			.unwrap();
		assert_eq!(out, val("[person:sam]").await);
	}

	// =========================================================================
	// Optional and the current value
	// =========================================================================

	#[tokio::test]
	async fn a_trailing_optional_compiles_its_body_as_the_current_value() {
		let ctx = root_ctx();
		let doc = val("{ a: NULL, b: 1 }").await;
		assert_eq!(eval_on("a.?", &doc, &ctx).await.unwrap(), Value::Null);
		assert_eq!(eval_on("b.?", &doc, &ctx).await.unwrap(), Value::from(1));
	}

	#[tokio::test]
	async fn current_reads_the_context_value_and_renders_as_nothing() {
		let ctx = root_ctx();
		let value = val("{ a: 1 }").await;
		let out =
			Current.evaluate(EvalContext::from_exec_ctx(&ctx).with_value(&value)).await.unwrap();
		assert_eq!(out, value);
		assert_eq!(Current.evaluate(EvalContext::from_exec_ctx(&ctx)).await.unwrap(), Value::None);
		assert_eq!(Current.to_sql(), "");
	}

	// =========================================================================
	// Plan metadata
	// =========================================================================

	#[tokio::test]
	async fn metadata_is_the_maximum_over_root_and_parts() {
		let ctx = root_ctx();
		let expr = physical_expr("people[WHERE age > 30]", &ctx).await;
		let step = expr.downcast_ref::<Step>().expect("a filter over a field is one step run");
		assert!(step.root().is_none());
		assert_eq!(step.parts().len(), 2);
		assert!(step.parts()[0].downcast_ref::<FieldPart>().is_some());
		assert!(step.parts()[1].downcast_ref::<WherePart>().is_some());
		// A field read may dereference a record id.
		assert_eq!(expr.required_context(), ContextLevel::Database);
		assert_eq!(expr.access_mode(), AccessMode::ReadOnly);
	}
}
