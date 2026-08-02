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
//! The write itself reuses the native document pipeline via [`legacy_compute`]:
//! a synthetic core `UpdateStatement` / `DeleteStatement` targeting the resolved
//! record id is run through `Expr::compute`, so table/field permissions, field
//! validation, events, indexes, references, and live-query notifications all
//! apply exactly as for a native mutation. Row-scoped value expressions are
//! evaluated against the current binding row first (with the row as the cursor
//! document), then embedded as concrete literals in the statement.

use std::borrow::Cow;
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;
use surrealdb_types::ToSql;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::operators::check_cancelled;
use crate::exec::plan_or_compute::{get_legacy_context, legacy_compute};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::data::Assignment;
use crate::expr::match_plan::{DetachMode, UpdateData};
use crate::expr::statements::{CreateStatement, DeleteStatement, RelateStatement, UpdateStatement};
use crate::expr::{AssignOperator, ControlFlow, Data, Expr, Literal, Output};
use crate::key::schema::GraphIdPrefix;
use crate::kvs::Direction;
use crate::val::{Object, RecordId, TableName, Value};

/// `SET` / `REMOVE` over the binding bound at `target`.
#[derive(Debug, Clone)]
pub struct UpdateBinding {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The binding name whose record is updated.
	pub(crate) target: String,
	/// The update to apply (row-scoped value expressions, evaluated per row).
	pub(crate) data: UpdateData,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl UpdateBinding {
	pub(crate) fn new(input: Arc<dyn ExecOperator>, target: String, data: UpdateData) -> Self {
		Self {
			input,
			target,
			data,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl ExecOperator for UpdateBinding {
	fn name(&self) -> &'static str {
		"UpdateBinding"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let kind = match &self.data {
			UpdateData::Set(_) => "set",
			UpdateData::Unset(_) => "unset",
			UpdateData::Content(_) => "content",
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

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let (opt, frozen) = legacy_handles(ctx)?;
		let target = self.target.clone();
		let data = self.data.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// Pipeline breaker: drain the whole input before writing.
			let mut rows = Vec::new();
			while let Some(batch) = input_stream.next().await {
				check_cancelled(&ctx)?;
				rows.extend(batch?.values);
			}
			let mut out = Vec::with_capacity(rows.len());
			// Apply per row, in textual row order: a row-scoped value (`SET a.x =
			// b.q`) is evaluated against each row, so a fan-out that binds the same
			// record more than once is last-write-wins (matching Cypher), and every
			// row sees a consistent post-mutation image of its own write. A null /
			// unresolvable target binding is passed through untouched.
			for mut row in rows {
				check_cancelled(&ctx)?;
				if let Some(rid) = target_record_id(&row, &target) {
					let after = apply_update(&data, &rid, &row, &frozen, &opt).await?;
					set_binding(&mut row, &target, after);
				}
				out.push(row);
			}
			yielder
				.emit(ValueBatch {
					values: out,
				})
				.await;
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "UpdateBinding", &self.metrics))
	}
}

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

impl DeleteBinding {
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
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let (opt, frozen) = legacy_handles(ctx)?;
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
				rows.extend(batch?.values);
			}
			let mut out = Vec::with_capacity(rows.len());
			// Apply per row, in textual row order. A fan-out can bind the SAME
			// record on several rows; the actual write runs once per distinct record
			// (a repeat is a pure no-op — the record is already gone), so deduping
			// the write also skips the redundant connected-edge probe a repeat would
			// otherwise re-run. Every row still gets its own NULL image below.
			let mut deleted: std::collections::HashSet<RecordId> = std::collections::HashSet::new();
			for mut row in rows {
				check_cancelled(&ctx)?;
				if let Some(rid) = target_record_id(&row, &target) {
					if deleted.insert(rid.clone()) {
						apply_delete(&rid, detach, is_edge, ns, db, &frozen, &opt).await?;
					}
					// The record is gone; surface NULL for it, and for any other
					// binding in this row that is an edge incident to it (a `DETACH`
					// delete cascades those edges, so they no longer exist).
					set_binding(&mut row, &target, Value::Null);
					null_incident_edges(&mut row, &rid);
				}
				out.push(row);
			}
			yielder
				.emit(ValueBatch {
					values: out,
				})
				.await;
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "DeleteBinding", &self.metrics))
	}
}

// ============================================================================
// Shared helpers
// ============================================================================

/// Extract the legacy `Options` (owned) and `FrozenContext` the per-row writes
/// need, mapping the lookup failure into a control-flow error.
fn legacy_handles(ctx: &ExecutionContext) -> FlowResult<(Options, FrozenContext)> {
	let (opt, frozen) =
		get_legacy_context(ctx).map_err(|e| ControlFlow::Err(anyhow::anyhow!(e)))?;
	Ok((opt, frozen))
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

/// Build the synthetic `UpdateStatement` for one resolved record, run it through
/// the native pipeline, and return its AFTER image (or `Value::Null`).
async fn apply_update(
	data: &UpdateData,
	rid: &RecordId,
	row: &Value,
	frozen: &FrozenContext,
	opt: &Options,
) -> Result<Value, ControlFlow> {
	let update_data = match data {
		UpdateData::Set(assignments) => {
			// Every assignment in one `SET` evaluates against the same pre-write row
			// image, so build the cursor document once (a single clone of the
			// binding row) and reuse it across all assignments rather than cloning
			// the row per assignment.
			let cursor = CursorDoc::new(None, None, row.clone());
			let mut assigns = Vec::with_capacity(assignments.len());
			for (place, value_expr) in assignments {
				let value = legacy_compute(value_expr, frozen, opt, Some(&cursor)).await?;
				assigns.push(Assignment {
					place: place.clone(),
					operator: AssignOperator::Assign,
					value: value.into_literal(),
				});
			}
			Data::SetExpression(assigns)
		}
		UpdateData::Unset(fields) => Data::UnsetExpression(fields.clone()),
		UpdateData::Content(object_expr) => {
			let value = eval_in_row(object_expr, row, frozen, opt).await?;
			Data::ContentExpression(value.into_literal())
		}
	};
	let stmt = UpdateStatement {
		what: vec![record_id_expr(rid)],
		data: Some(update_data),
		output: Some(Output::After),
		..Default::default()
	};
	let result = legacy_compute(&Expr::Update(Box::new(stmt)), frozen, opt, None).await?;
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
	frozen: &FrozenContext,
	opt: &Options,
) -> Result<(), ControlFlow> {
	// `DETACH`/`NODETACH` govern deleting a NODE that still has relationships.
	// Deleting an edge just removes the relationship (native DELETE purges the
	// edge's own graph keys), so the connected-edge probe is skipped — an edge's
	// `<->` adjacency is its endpoint vertices, not sub-edges.
	if !is_edge
		&& matches!(detach, DetachMode::NoDetach)
		&& has_connected_edges(rid, ns, db, frozen).await?
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
		what: vec![record_id_expr(rid)],
		..Default::default()
	};
	legacy_compute(&Expr::Delete(Box::new(stmt)), frozen, opt, None).await?;
	Ok(())
}

/// Whether `rid` has any connected graph edge (in either direction).
///
/// Peeks the record's graph-key range directly off the transaction — exactly the
/// permission-independent check the native cascade uses
/// (`doc::purge::purge_edges`), fetching at most one key. This deliberately does
/// NOT go through a SELECT/idiom read: the `NODETACH` guard must reflect the
/// record's actual adjacency, never the caller's SELECT visibility, or a
/// record-scoped user who cannot read the incident edges would slip past the
/// guard and the native DELETE would silently cascade them.
async fn has_connected_edges(
	rid: &RecordId,
	ns: NamespaceId,
	db: DatabaseId,
	frozen: &FrozenContext,
) -> Result<bool, ControlFlow> {
	let txn = frozen.tx();
	let range = GraphIdPrefix {
		ns,
		db,
		tb: Cow::Borrowed(&rid.table),
		id: Cow::Borrowed(&rid.key),
	}
	.range()?;
	let mut cursor = txn
		.open_keys_cursor_raw(range, Direction::Forward, 0, None)
		.await
		.map_err(ControlFlow::Err)?;
	let batch = cursor.next_batch(1).await.map_err(ControlFlow::Err)?;
	Ok(!batch.is_empty())
}

/// A record-id target expression for a synthetic statement's `what`.
fn record_id_expr(rid: &RecordId) -> Expr {
	Expr::Literal(Literal::RecordId(rid.clone().into_literal()))
}

/// Extract the single record from a mutation result (native CREATE/UPDATE return
/// an array of affected records); `Value::Null` when empty.
fn single_record(value: Value) -> Value {
	match value {
		Value::Array(mut arr) => arr.0.drain(..).next().unwrap_or(Value::Null),
		other => other,
	}
}

/// Evaluate a binding-row-scoped expression against `row` (the row is the cursor
/// document, so `a.x` reads `row.a.x`), reusing the legacy compute path.
async fn eval_in_row(
	expr: &Expr,
	row: &Value,
	frozen: &FrozenContext,
	opt: &Options,
) -> Result<Value, ControlFlow> {
	let cursor = CursorDoc::new(None, None, row.clone());
	legacy_compute(expr, frozen, opt, Some(&cursor)).await
}

// ============================================================================
// INSERT
// ============================================================================

/// A new node an [`InsertGraph`] creates.
#[derive(Debug, Clone)]
pub(crate) struct InsertNodeOp {
	/// The binding name the created record is bound under.
	pub(crate) name: String,
	/// The target table (= label).
	pub(crate) table: TableName,
	/// The row-scoped property object, evaluated per row.
	pub(crate) props: Expr,
}

/// A new edge an [`InsertGraph`] relates between two endpoints (each a node
/// created by this stage or a read-bound reference), addressed by binding name.
#[derive(Debug, Clone)]
pub(crate) struct InsertEdgeOp {
	pub(crate) name: String,
	pub(crate) table: TableName,
	pub(crate) from: String,
	pub(crate) to: String,
	pub(crate) props: Expr,
}

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

impl InsertGraph {
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

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let (opt, frozen) = legacy_handles(ctx)?;
		let nodes = self.nodes.clone();
		let edges = self.edges.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let mut rows = Vec::new();
			while let Some(batch) = input_stream.next().await {
				check_cancelled(&ctx)?;
				rows.extend(batch?.values);
			}
			let mut out = Vec::with_capacity(rows.len());
			for mut row in rows {
				check_cancelled(&ctx)?;
				insert_row(&nodes, &edges, &mut row, &frozen, &opt).await?;
				out.push(row);
			}
			yielder
				.emit(ValueBatch {
					values: out,
				})
				.await;
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "InsertGraph", &self.metrics))
	}
}

/// Create the new nodes (binding each into the row so later nodes/edges can
/// reference it) and relate the edges, for one input row.
async fn insert_row(
	nodes: &[InsertNodeOp],
	edges: &[InsertEdgeOp],
	row: &mut Value,
	frozen: &FrozenContext,
	opt: &Options,
) -> Result<(), ControlFlow> {
	for node in nodes {
		let props = eval_in_row(&node.props, row, frozen, opt).await?;
		let stmt = CreateStatement {
			what: vec![Expr::Table(node.table.clone())],
			data: Some(Data::ContentExpression(props.into_literal())),
			output: Some(Output::After),
			..Default::default()
		};
		let result = legacy_compute(&Expr::Create(Box::new(stmt)), frozen, opt, None).await?;
		set_binding(row, &node.name, single_record(result));
	}
	for edge in edges {
		let from = target_record_id(row, &edge.from).ok_or_else(|| endpoint_error(&edge.from))?;
		let to = target_record_id(row, &edge.to).ok_or_else(|| endpoint_error(&edge.to))?;
		let props = eval_in_row(&edge.props, row, frozen, opt).await?;
		let stmt = RelateStatement {
			only: false,
			or_update: false,
			through: Expr::Table(edge.table.clone()),
			from: record_id_expr(&from),
			to: record_id_expr(&to),
			data: Some(Data::ContentExpression(props.into_literal())),
			output: Some(Output::After),
			timeout: Expr::Literal(Literal::None),
		};
		let result = legacy_compute(&Expr::Relate(Box::new(stmt)), frozen, opt, None).await?;
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

/// Emits exactly one empty binding row — the seed for a leading `INSERT` (one
/// with no `MATCH` read body), so the `INSERT` runs exactly once. (`EmptyScan`
/// emits zero rows and so cannot seed a mutation.)
#[derive(Debug, Clone)]
pub struct SingleRowScan {
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SingleRowScan {
	pub(crate) fn new() -> Self {
		Self {
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl Default for SingleRowScan {
	fn default() -> Self {
		Self::new()
	}
}

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
		let batch = ValueBatch {
			values: vec![Value::Object(Object::default())],
		};
		let stream = futures::stream::once(async move { Ok(batch) });
		Ok(monitor_stream(Box::pin(stream), "SingleRowScan", &self.metrics))
	}
}

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

impl DrainSink {
	pub(crate) fn new(input: Arc<dyn ExecOperator>) -> Self {
		Self {
			input,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

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
			self.input.access_mode(),
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
