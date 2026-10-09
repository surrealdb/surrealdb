//! GQL mutation operators over a binding table: `SET`/`REMOVE` ([`UpdateBinding`])
//! and `DELETE` ([`DeleteBinding`]).
//!
//! Both are **pipeline breakers**: they fully drain their input binding rows
//! before applying any write, mirroring the native `Iterator::prepare`-then-
//! `output` split. This is what makes them Halloween-safe — a still-open
//! upstream scan can never re-observe a row this operator just wrote. They then
//! apply per row in textual row order: a row-scoped value (`SET a.x = b.q`) is
//! evaluated against each row, so a fan-out binding the same record more than
//! once is last-write-wins (matching Cypher) and every row carries a consistent
//! image of its own write.
//!
//! The write itself is the statement a hand-written mutation would be: a
//! synthetic `UpdateStatement` / `DeleteStatement` / `CreateStatement` /
//! `RelateStatement` targeting the resolved record, planned by the
//! planner and run in this operator's execution context ([`run_write`]), so
//! table/field permissions, field validation, events, indexes, references, and
//! live-query notifications apply exactly as for that statement. The
//! row-scoped values a stage writes are compiled [`PhysicalExpr`]s: each is
//! evaluated against the current binding row, and the resulting value is
//! embedded as a literal in the synthetic statement.

use std::borrow::Cow;
use std::sync::Arc;

#[cfg(test)]
use common::future::stream::{self, Yielder};
#[cfg(test)]
use futures::StreamExt;
use surrealdb_types::ToSql;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
#[cfg(test)]
use crate::exec::operators::check_cancelled;
#[cfg(test)]
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, OperatorMetrics,
	ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::exec::{ExecutionContext, FlowResult, PhysicalExpr};
use crate::expr::data::Assignment;
use crate::expr::match_plan::DetachMode;
use crate::expr::statements::{CreateStatement, DeleteStatement, RelateStatement, UpdateStatement};
use crate::expr::{AssignOperator, ControlFlow, Data, Expr, Idiom, Literal, Output};
use crate::idx::adjacency::{
	AdjacencyScope, MergedAdjacencyCursor, VertexAdjacency, vertex_adjacency,
};
use crate::key::schema::GraphIdPrefix;
#[cfg(test)]
use crate::val::Object;
use crate::val::{RecordId, TableName, Value};

/// The compiled update an [`UpdateBinding`] applies. A property is a single
/// field name on the target record; each value `V` is a row-scoped expression
/// evaluated against the binding row before the write.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum UpdateOp<V = Arc<dyn PhysicalExpr>> {
	/// `SET a.p = v`, one `(property, value)` per assignment.
	Set(Vec<(String, V)>),
	/// `REMOVE a.p`, one property per removal.
	Unset(Vec<String>),
	/// `SET a = {…}`: the object replacing every user property.
	Content(V),
}

#[cfg(test)]
/// `SET` / `REMOVE` over the binding bound at `target`.
#[derive(Debug, Clone)]
pub struct UpdateBinding {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The binding name whose record is updated.
	pub(crate) target: String,
	/// The update to apply (row-scoped values, evaluated per row).
	pub(crate) op: UpdateOp,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl UpdateBinding {
	#[cfg(test)]
	fn new(input: Arc<dyn ExecOperator>, target: String, op: UpdateOp) -> Self {
		Self {
			input,
			target,
			op,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	#[cfg(test)]
	/// `SET target.p = v` for each `(p, v)` in `assignments`.
	pub(crate) fn set(
		input: Arc<dyn ExecOperator>,
		target: String,
		assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
	) -> Self {
		Self::new(input, target, UpdateOp::Set(assignments))
	}

	#[cfg(test)]
	/// `REMOVE target.p` for each `p` in `properties`.
	pub(crate) fn unset(
		input: Arc<dyn ExecOperator>,
		target: String,
		properties: Vec<String>,
	) -> Self {
		Self::new(input, target, UpdateOp::Unset(properties))
	}

	#[cfg(test)]
	/// `SET target = object`.
	pub(crate) fn content(
		input: Arc<dyn ExecOperator>,
		target: String,
		object: Arc<dyn PhysicalExpr>,
	) -> Self {
		Self::new(input, target, UpdateOp::Content(object))
	}
}

#[cfg(test)]
impl ExecOperator for UpdateBinding {
	fn name(&self) -> &'static str {
		"UpdateBinding"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let kind = match &self.op {
			UpdateOp::Set(_) => "set",
			UpdateOp::Unset(_) => "unset",
			UpdateOp::Content(_) => "content",
		};
		vec![("binding".to_string(), self.target.clone()), ("op".to_string(), kind.to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadWrite
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		match &self.op {
			UpdateOp::Set(assignments) => {
				assignments.iter().map(|(property, value)| (property.as_str(), value)).collect()
			}
			UpdateOp::Unset(_) => Vec::new(),
			UpdateOp::Content(object) => vec![("content", object)],
		}
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.access_mode().combine(self.input.access_mode()),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let target = self.target.clone();
		let op = self.op.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// Pipeline breaker: drain the whole input before writing.
			let mut rows = Vec::new();
			while let Some(batch) = input_stream.next().await {
				check_cancelled(&ctx)?;
				rows.extend(batch?.into_values());
			}
			let mut out = Vec::with_capacity(rows.len());
			// Apply per row, in textual row order: a row-scoped value (`SET a.x =
			// b.q`) is evaluated against each row, so a fan-out that binds the same
			// record more than once is last-write-wins (matching Cypher), and every
			// row sees a consistent post-mutation image of its own write. A null /
			// unresolvable target binding is passed through untouched.
			let mut eval = PhysicalEval(&ctx);
			let mut plans = WritePlans::default();
			for mut row in rows {
				check_cancelled(&ctx)?;
				update_row(&op, &target, &mut row, &mut eval, &mut plans, &ctx).await?;
				out.push(row);
			}
			yielder.emit(ValueBatch::new(out)).await;
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "UpdateBinding", &self.metrics))
	}
}

#[cfg(test)]
/// `[DETACH|NODETACH] DELETE` of the binding bound at `target`.
#[derive(Debug, Clone)]
pub struct DeleteBinding {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) target: String,
	pub(crate) detach: DetachMode,
	/// Whether the target binding is an edge. `DETACH`/`NODETACH` apply only to
	/// nodes (deleting a node with relationships); deleting an edge just removes
	/// the relationship, so the connected-edge probe is skipped for edges.
	pub(crate) is_edge: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl DeleteBinding {
	#[cfg(test)]
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		target: String,
		detach: DetachMode,
		is_edge: bool,
	) -> Self {
		Self {
			input,
			target,
			detach,
			is_edge,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg(test)]
impl ExecOperator for DeleteBinding {
	fn name(&self) -> &'static str {
		"DeleteBinding"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let detach = match self.detach {
			DetachMode::Detach => "detach",
			DetachMode::NoDetach => "nodetach",
		};
		vec![("binding".to_string(), self.target.clone()), ("mode".to_string(), detach.to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadWrite
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.access_mode().combine(self.input.access_mode()),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		// Namespace/database ids for the connected-edge graph-key peek (resolved
		// once from the already-loaded database context, not per row).
		let (ns, db) = {
			let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(anyhow::anyhow!(e)))?;
			(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id)
		};
		let target = self.target.clone();
		let detach = self.detach;
		let is_edge = self.is_edge;
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let mut rows = Vec::new();
			while let Some(batch) = input_stream.next().await {
				check_cancelled(&ctx)?;
				rows.extend(batch?.into_values());
			}
			let mut out = Vec::with_capacity(rows.len());
			// Apply per row, in textual row order. A fan-out can bind the SAME
			// record on several rows; the actual write runs once per distinct record
			// (a repeat is a pure no-op — the record is already gone), so deduping
			// the write also skips the redundant connected-edge probe a repeat would
			// otherwise re-run. Every row still gets its own NULL image below.
			let mut deleted: std::collections::HashSet<RecordId> = std::collections::HashSet::new();
			let mut plans = WritePlans::default();
			for mut row in rows {
				check_cancelled(&ctx)?;
				delete_row(
					&mut row,
					&target,
					detach,
					is_edge,
					(ns, db),
					&mut deleted,
					&mut plans,
					&ctx,
				)
				.await?;
				out.push(row);
			}
			yielder.emit(ValueBatch::new(out)).await;
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "DeleteBinding", &self.metrics))
	}
}

// ============================================================================
// Shared helpers
// ============================================================================

#[cfg(test)]
thread_local! {
	/// How many synthetic writes this thread's mutation steps took.
	pub(crate) static WRITES_PLANNED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The plans of the synthetic statements one mutation step applies to the
/// records of its rows, by the statement as written.
///
/// A step's statement differs between rows only in its values — the record
/// it writes and the data it writes there — which are parameters of the
/// statement ([`Param`]), so its plan serves every row: the step takes it from
/// the compile cache when its first row reaches it, compiled on the cache's
/// miss once per node and frame, and every row runs it with its own values
/// bound. The plan is an operator-tree plan, which a program, plain data,
/// cannot hold; so the statement is planned by the cache rather than with the
/// program that holds the step.
#[derive(Default)]
pub(crate) struct WritePlans {
	plans: Vec<(Expr, Arc<dyn crate::exec::ExecOperator>)>,
}

/// The names a mutation's synthetic statement reads its per-row values from.
#[derive(Clone, Copy)]
enum Param {
	Record,
	From,
	To,
	Data,
	Value(usize),
}

impl Param {
	fn name(self) -> String {
		match self {
			Param::Record => "__mutation_record".to_owned(),
			Param::From => "__mutation_from".to_owned(),
			Param::To => "__mutation_to".to_owned(),
			Param::Data => "__mutation_data".to_owned(),
			Param::Value(i) => format!("__mutation_value_{i}"),
		}
	}

	fn expr(self) -> Expr {
		Expr::Param(self.name().into())
	}
}

/// Run `write`, the synthetic statement a mutation applies to one record, in
/// `ctx`, with `values` bound to the parameters it reads: planned once per
/// statement through `plans`, as that statement written by hand would be,
/// and its value collected as that statement's.
async fn run_write(
	write: Expr,
	values: Vec<(Param, Value)>,
	plans: &mut WritePlans,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	let plan = match plans.plans.iter().find(|(held, _)| *held == write) {
		Some((_, plan)) => Arc::clone(plan),
		None => {
			let op = crate::expr::logical::lower(write.clone()).map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(format!(
					"a mutation's write did not lower: {e}"
				))))
			})?;
			// Every per-row value is read when the plan runs.
			let shadowed: Vec<String> = values.iter().map(|(param, _)| param.name()).collect();
			// Served from the compile cache by the statement as written, the
			// per-row values its parameters: compiled once per node and frame,
			// and taken by every step that applies it
			// ([`crate::exec::vm::serve`]).
			let plan = crate::exec::vm::serve::statement(
				ctx,
				op,
				Some(Arc::clone(&ctx.root().auth)),
				0,
				shadowed,
			)
			.await
			.map_err(ControlFlow::Err)?;
			#[cfg(test)]
			WRITES_PLANNED.with(|planned| planned.set(planned.get() + 1));
			plans.plans.push((write, Arc::clone(&plan)));
			plan
		}
	};
	let ctx = ctx.with_params(values.into_iter().map(|(param, value)| (param.name(), value)));
	let shape = plan.output_shape();
	crate::exec::operators::collect_shaped_value(plan.execute(&ctx)?, shape).await
}

/// Recover the record id of the node/edge bound at `name` via the shared
/// [`crate::exec::operators::binding_record_id`] (the binding holds the full
/// record object, so `obj.id` is the id, or — for a hidden edge — a bare record
/// id). A null / missing binding yields `None` and the row passes through
/// untouched.
fn target_record_id(row: &Value, name: &str) -> Option<RecordId> {
	crate::exec::operators::binding_record_id(row, name)
}

/// Rebind `row[name]` to `value` (the post-mutation image), if the row is an
/// object.
fn set_binding(row: &mut Value, name: &str, value: Value) {
	if let Value::Object(obj) = row {
		obj.insert(name.to_string(), value);
	}
}

/// Null every binding in `row` that is an edge object incident to `deleted`
/// (its `in` or `out` is the deleted record) — those edges were cascaded by a
/// `DETACH` delete, so a trailing `RETURN` must not surface the stale edge.
/// Hidden (id-only) edge bindings are never user-projected, so only full edge
/// objects need handling.
fn null_incident_edges(row: &mut Value, deleted: &RecordId) {
	let Value::Object(obj) = row else {
		return;
	};
	let deleted = Value::RecordId(deleted.clone());
	for value in obj.values_mut() {
		if let Value::Object(edge) = value
			&& (edge.get("in") == Some(&deleted) || edge.get("out") == Some(&deleted))
		{
			*value = Value::Null;
		}
	}
}

/// Evaluates the row-scoped values of a mutation stage against a binding row.
/// The values are expressions the tree compiled, or scalars a program holds.
pub(crate) trait RowEval<V> {
	/// `value` evaluated with `row` as the current value and the document root,
	/// so `a.x` reads `row.a.x`.
	fn eval<'a>(
		&'a mut self,
		value: &'a V,
		row: &'a Value,
	) -> futures::future::BoxFuture<'a, Result<Value, ControlFlow>>;
}

#[cfg(test)]
/// The values of the operator tree: compiled expressions, evaluated in the
/// execution context.
struct PhysicalEval<'c>(&'c ExecutionContext);

#[cfg(test)]
impl RowEval<Arc<dyn PhysicalExpr>> for PhysicalEval<'_> {
	fn eval<'a>(
		&'a mut self,
		value: &'a Arc<dyn PhysicalExpr>,
		row: &'a Value,
	) -> futures::future::BoxFuture<'a, Result<Value, ControlFlow>> {
		Box::pin(eval_in_row(value, row, self.0))
	}
}

/// Apply `op` to the record bound at `target` in `row`, rebinding `row[target]`
/// to the record's AFTER image. A null / unresolvable target binding is passed
/// through untouched.
pub(crate) async fn update_row<V>(
	op: &UpdateOp<V>,
	target: &str,
	row: &mut Value,
	eval: &mut impl RowEval<V>,
	plans: &mut WritePlans,
	ctx: &ExecutionContext,
) -> Result<(), ControlFlow> {
	if let Some(rid) = target_record_id(row, target) {
		let after = apply_update(op, &rid, row, eval, plans, ctx).await?;
		set_binding(row, target, after);
	}
	Ok(())
}

/// Delete the record bound at `target` in `row`, once per distinct record in
/// `deleted`, and rebind it to `NULL`, as every edge of the row incident to it.
/// `(ns, db)` are the ids of the database the connected-edge probe reads.
#[expect(
	clippy::too_many_arguments,
	reason = "the row, its binding and the step's mode, and the state the step keeps across rows"
)]
pub(crate) async fn delete_row(
	row: &mut Value,
	target: &str,
	detach: DetachMode,
	is_edge: bool,
	(ns, db): (NamespaceId, DatabaseId),
	deleted: &mut std::collections::HashSet<RecordId>,
	plans: &mut WritePlans,
	ctx: &ExecutionContext,
) -> Result<(), ControlFlow> {
	if let Some(rid) = target_record_id(row, target) {
		if deleted.insert(rid.clone()) {
			apply_delete(&rid, detach, is_edge, ns, db, plans, ctx).await?;
		}
		// The record is gone; surface NULL for it, and for any other binding in
		// this row that is an edge incident to it (a `DETACH` delete cascades
		// those edges, so they no longer exist).
		set_binding(row, target, Value::Null);
		null_incident_edges(row, &rid);
	}
	Ok(())
}

/// Build the synthetic `UpdateStatement` for one resolved record, run it through
/// the native pipeline, and return its AFTER image (or `Value::Null`).
async fn apply_update<V>(
	op: &UpdateOp<V>,
	rid: &RecordId,
	row: &Value,
	eval: &mut impl RowEval<V>,
	plans: &mut WritePlans,
	ctx: &ExecutionContext,
) -> Result<Value, ControlFlow> {
	let mut values = vec![(Param::Record, Value::RecordId(rid.clone()))];
	let update_data = match op {
		UpdateOp::Set(assignments) => {
			// Every assignment in one `SET` evaluates against the same pre-write
			// row image.
			let mut assigns = Vec::with_capacity(assignments.len());
			for (i, (property, value)) in assignments.iter().enumerate() {
				values.push((Param::Value(i), eval.eval(value, row).await?));
				assigns.push(Assignment {
					place: Idiom::field(property.clone()),
					operator: AssignOperator::Assign,
					value: Param::Value(i).expr(),
				});
			}
			Data::SetExpression(assigns)
		}
		UpdateOp::Unset(properties) => Data::UnsetExpression(
			properties.iter().map(|property| Idiom::field(property.clone())).collect(),
		),
		UpdateOp::Content(object) => {
			values.push((Param::Data, eval.eval(object, row).await?));
			Data::ContentExpression(Param::Data.expr())
		}
	};
	let stmt = UpdateStatement {
		what: vec![Param::Record.expr()],
		data: Some(update_data),
		output: Some(Output::After),
		..Default::default()
	};
	let result = run_write(Expr::Update(Box::new(stmt)), values, plans, ctx).await?;
	Ok(single_record(result))
}

/// Run the synthetic `DeleteStatement` for one resolved record. `NoDetach` (the
/// ISO default) first errors if the node still has connected edges; `Detach`
/// cascades them (native `DELETE` always purges connected edges).
async fn apply_delete(
	rid: &RecordId,
	detach: DetachMode,
	is_edge: bool,
	ns: NamespaceId,
	db: DatabaseId,
	plans: &mut WritePlans,
	ctx: &ExecutionContext,
) -> Result<(), ControlFlow> {
	// `DETACH`/`NODETACH` govern deleting a NODE that still has relationships.
	// Deleting an edge just removes the relationship (native DELETE purges the
	// edge's own graph keys), so the connected-edge probe is skipped — an edge's
	// `<->` adjacency is its endpoint vertices, not sub-edges.
	if !is_edge
		&& matches!(detach, DetachMode::NoDetach)
		&& has_connected_edges(rid, ns, db, ctx.ctx()).await?
	{
		return Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(
			format!(
				"Cannot DELETE `{}` because it still has connected edges; use `DETACH DELETE` to \
				 remove the edges as well",
				rid.to_sql()
			)
		))));
	}
	let stmt = DeleteStatement {
		what: vec![Param::Record.expr()],
		..Default::default()
	};
	let values = vec![(Param::Record, Value::RecordId(rid.clone()))];
	run_write(Expr::Delete(Box::new(stmt)), values, plans, ctx).await?;
	Ok(())
}

/// Whether `rid` has any connected graph edge (in either direction).
///
/// Peeks the record's adjacency through the merged reader — exactly the
/// permission-independent check the native cascade uses
/// (`doc::purge::purge_edges`), fetching at most one entry across the
/// per-edge pointer keys and, once the table has folded, the packed blocks
/// (tombstones subtracted, so deleted-but-unfolded edges do not trip the
/// guard). This deliberately does NOT go through a SELECT/idiom read: the
/// `NODETACH` guard must reflect the record's actual adjacency, never the
/// caller's SELECT visibility, or a record-scoped user who cannot read the
/// incident edges would slip past the guard and the native DELETE would
/// silently cascade them.
async fn has_connected_edges(
	rid: &RecordId,
	ns: NamespaceId,
	db: DatabaseId,
	frozen: &FrozenContext,
) -> Result<bool, ControlFlow> {
	let txn = frozen.tx();
	let adjacency = vertex_adjacency(&txn, ns, db, &rid.table).await.map_err(ControlFlow::Err)?;
	let scope = AdjacencyScope {
		ns,
		db,
		vertex: rid,
		dir: None,
		edge_table: None,
		fk_lower: None,
		delta_range: GraphIdPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
		}
		.range()?,
	};
	let mut cursor = match adjacency {
		// A lightweight edge's synthesized adjacency mirrors the edge-side
		// keys a classic edge carries, so NODETACH treats both alike.
		VertexAdjacency::Lightweight => {
			MergedAdjacencyCursor::open_lightweight(&txn, &scope).map_err(ControlFlow::Err)?
		}
		VertexAdjacency::Normal {
			folded,
		} => MergedAdjacencyCursor::open(
			&txn,
			scope,
			folded,
			None,
			Some(frozen.get_index_stores().adjacency_resolve()),
		)
		.await
		.map_err(ControlFlow::Err)?,
	};
	// Only emptiness matters here, so nothing is decoded.
	let batch = cursor.next_batch_keys(1).await.map_err(ControlFlow::Err)?;
	Ok(!batch.is_empty())
}

/// Extract the single record from a mutation result (native CREATE/UPDATE return
/// an array of affected records); `Value::Null` when empty.
fn single_record(value: Value) -> Value {
	match value {
		Value::Array(mut arr) => arr.0.drain(..).next().unwrap_or(Value::Null),
		other => other,
	}
}

#[cfg(test)]
/// Evaluate a binding-row-scoped expression against `row` (the row is the
/// current value and the document root, so `a.x` reads `row.a.x`).
async fn eval_in_row(
	expr: &Arc<dyn PhysicalExpr>,
	row: &Value,
	ctx: &ExecutionContext,
) -> Result<Value, ControlFlow> {
	let eval = EvalContext::from_exec_ctx(ctx).with_value_and_doc(row);
	expr.evaluate(eval).await
}

// ============================================================================
// INSERT
// ============================================================================

/// A new node an [`InsertGraph`] creates.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertNodeOp<V = Arc<dyn PhysicalExpr>> {
	/// The binding name the created record is bound under.
	pub(crate) name: String,
	/// The target table (= label).
	pub(crate) table: TableName,
	/// The row-scoped property object, evaluated per row.
	pub(crate) props: V,
}

/// A new edge an [`InsertGraph`] relates between two endpoints (each a node
/// created by this stage or a read-bound reference), addressed by binding name.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertEdgeOp<V = Arc<dyn PhysicalExpr>> {
	pub(crate) name: String,
	pub(crate) table: TableName,
	pub(crate) from: String,
	pub(crate) to: String,
	/// The row-scoped property object, evaluated per row.
	pub(crate) props: V,
}

#[cfg(test)]
/// `INSERT` of new nodes and edges. Per input row (or once, when seeded by a
/// [`SingleRowScan`] for a leading `INSERT` with no `MATCH`): create each new
/// node and bind it into the row, relate each edge between the resolved
/// endpoints, then emit the extended row for a trailing `RETURN`. A pipeline
/// breaker, like the other mutation operators.
#[derive(Debug, Clone)]
pub struct InsertGraph {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) nodes: Vec<InsertNodeOp>,
	pub(crate) edges: Vec<InsertEdgeOp>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl InsertGraph {
	#[cfg(test)]
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		nodes: Vec<InsertNodeOp>,
		edges: Vec<InsertEdgeOp>,
	) -> Self {
		Self {
			input,
			nodes,
			edges,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg(test)]
impl ExecOperator for InsertGraph {
	fn name(&self) -> &'static str {
		"InsertGraph"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("nodes".to_string(), self.nodes.len().to_string()),
			("edges".to_string(), self.edges.len().to_string()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadWrite
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		self.nodes
			.iter()
			.map(|node| (node.name.as_str(), &node.props))
			.chain(self.edges.iter().map(|edge| (edge.name.as_str(), &edge.props)))
			.collect()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.access_mode().combine(self.input.access_mode()),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let nodes = self.nodes.clone();
		let edges = self.edges.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let mut rows = Vec::new();
			while let Some(batch) = input_stream.next().await {
				check_cancelled(&ctx)?;
				rows.extend(batch?.into_values());
			}
			let mut out = Vec::with_capacity(rows.len());
			let mut eval = PhysicalEval(&ctx);
			let mut plans = WritePlans::default();
			for mut row in rows {
				check_cancelled(&ctx)?;
				insert_row(&nodes, &edges, &mut row, &mut eval, &mut plans, &ctx).await?;
				out.push(row);
			}
			yielder.emit(ValueBatch::new(out)).await;
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "InsertGraph", &self.metrics))
	}
}

/// Create the new nodes (binding each into the row so later nodes/edges can
/// reference it) and relate the edges, for one input row.
pub(crate) async fn insert_row<V>(
	nodes: &[InsertNodeOp<V>],
	edges: &[InsertEdgeOp<V>],
	row: &mut Value,
	eval: &mut impl RowEval<V>,
	plans: &mut WritePlans,
	ctx: &ExecutionContext,
) -> Result<(), ControlFlow> {
	for node in nodes {
		let props = eval.eval(&node.props, row).await?;
		let stmt = CreateStatement {
			what: vec![Expr::Table(node.table.clone())],
			data: Some(Data::ContentExpression(Param::Data.expr())),
			output: Some(Output::After),
			..Default::default()
		};
		let values = vec![(Param::Data, props)];
		let result = run_write(Expr::Create(Box::new(stmt)), values, plans, ctx).await?;
		set_binding(row, &node.name, single_record(result));
	}
	for edge in edges {
		let from = target_record_id(row, &edge.from).ok_or_else(|| endpoint_error(&edge.from))?;
		let to = target_record_id(row, &edge.to).ok_or_else(|| endpoint_error(&edge.to))?;
		let props = eval.eval(&edge.props, row).await?;
		let stmt = RelateStatement {
			only: false,
			or_update: false,
			through: Expr::Table(edge.table.clone()),
			from: Param::From.expr(),
			to: Param::To.expr(),
			data: Some(Data::ContentExpression(Param::Data.expr())),
			output: Some(Output::After),
			timeout: Expr::Literal(Literal::None),
		};
		let values = vec![
			(Param::From, Value::RecordId(from)),
			(Param::To, Value::RecordId(to)),
			(Param::Data, props),
		];
		let result = run_write(Expr::Relate(Box::new(stmt)), values, plans, ctx).await?;
		set_binding(row, &edge.name, single_record(result));
	}
	Ok(())
}

/// The error for an `INSERT` edge whose endpoint binding does not resolve to a
/// record (e.g. a referenced variable that is `NULL`).
fn endpoint_error(name: &str) -> ControlFlow {
	ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(format!(
		"INSERT edge endpoint `{name}` did not resolve to a record"
	))))
}

#[cfg(test)]
/// Emits exactly one empty binding row — the seed for a leading `INSERT` (one
/// with no `MATCH` read body), so the `INSERT` runs exactly once. (`EmptyScan`
/// emits zero rows and so cannot seed a mutation.)
#[derive(Debug, Clone)]
pub struct SingleRowScan {
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl SingleRowScan {
	#[cfg(test)]
	pub(crate) fn new() -> Self {
		Self {
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg(test)]
impl Default for SingleRowScan {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
impl ExecOperator for SingleRowScan {
	fn name(&self) -> &'static str {
		"SingleRowScan"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::Bounded(1)
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let batch = ValueBatch::new(vec![Value::Object(Object::default())]);
		let stream = futures::stream::once(async move { Ok(batch) });
		Ok(monitor_stream(Box::pin(stream), "SingleRowScan", &self.metrics))
	}
}

#[cfg(test)]
/// Drives its input to completion and emits no rows.
///
/// Used as the tail of a mutation-only GQL query (one with no `RETURN`): the
/// mutation operators below are pipeline breakers that perform their writes the
/// first time their output stream is polled, so the tail must still pull them to
/// completion — but the query returns an empty result, mirroring a native
/// `DELETE`/`UPDATE` with no output clause.
#[derive(Debug, Clone)]
pub struct DrainSink {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl DrainSink {
	#[cfg(test)]
	pub(crate) fn new(input: Arc<dyn ExecOperator>) -> Self {
		Self {
			input,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg(test)]
impl ExecOperator for DrainSink {
	fn name(&self) -> &'static str {
		"DrainSink"
	}

	fn required_context(&self) -> ContextLevel {
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.access_mode().combine(self.input.access_mode()),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let ctx = ctx.clone();
		let stream = stream::try_async_stream(async move |_: Yielder<_>| {
			while let Some(batch) = input_stream.next().await {
				check_cancelled(&ctx)?;
				// Drive the writes; discard the rows.
				let _ = batch?;
			}
			Ok(())
		});
		Ok(monitor_stream(Box::pin(stream), "DrainSink", &self.metrics))
	}
}
