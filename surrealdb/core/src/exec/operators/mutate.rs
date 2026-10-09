//! The row handling the VM's GQL mutation steps share: `SET`/`REMOVE`
//! ([`update_row`]) and `DELETE` ([`delete_row`]) over a binding table, generic
//! over how a row-scoped expression is evaluated ([`RowEval`]).
//!
//! The steps are **pipeline breakers**: they fully drain their input binding
//! rows before applying any write, which is what makes them Halloween-safe — a
//! still-open upstream scan can never re-observe a row a step just wrote. They
//! then apply per row in textual row order: a row-scoped value (`SET a.x = b.q`)
//! is evaluated against each row, so a fan-out binding the same record more
//! than once is last-write-wins (matching Cypher) and every row carries a
//! consistent image of its own write.
//!
//! The write itself is the statement a hand-written mutation would be: a
//! synthetic `UpdateStatement` / `DeleteStatement` / `CreateStatement` /
//! `RelateStatement` targeting the resolved record, planned and run in the
//! step's execution context ([`run_write`]), so table/field permissions, field
//! validation, events, indexes, references, and live-query notifications apply
//! exactly as for that statement. The row-scoped values a step writes are
//! evaluated against the current binding row, and the resulting value is
//! embedded as a literal in the synthetic statement.

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_types::ToSql;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::exec::{Clause, ExecutionContext, FlowResult};
use crate::expr::data::Assignment;
use crate::expr::match_plan::DetachMode;
use crate::expr::statements::{CreateStatement, DeleteStatement, RelateStatement, UpdateStatement};
use crate::expr::{AssignOperator, ControlFlow, Data, Expr, Idiom, Literal, Output};
use crate::idx::adjacency::{
	AdjacencyScope, MergedAdjacencyCursor, VertexAdjacency, vertex_adjacency,
};
use crate::key::schema::GraphIdPrefix;
use crate::val::{RecordId, TableName, Value};

/// The compiled update a mutation step applies. A property is a single
/// field name on the target record; each value `V` is a row-scoped expression
/// evaluated against the binding row before the write.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum UpdateOp<V = Clause> {
	/// `SET a.p = v`, one `(property, value)` per assignment.
	Set(Vec<(String, V)>),
	/// `REMOVE a.p`, one property per removal.
	Unset(Vec<String>),
	/// `SET a = {…}`: the object replacing every user property.
	Content(V),
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
/// bound. The plan is an [`ExecOperator`](crate::exec::ExecOperator), which a
/// program, plain data, cannot hold; so the statement is planned by the cache
/// rather than with the program that holds the step.
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
/// The values are compiled expressions, or scalars a program holds.
pub(crate) trait RowEval<V> {
	/// `value` evaluated with `row` as the current value and the document root,
	/// so `a.x` reads `row.a.x`.
	fn eval<'a>(
		&'a mut self,
		value: &'a V,
		row: &'a Value,
	) -> futures::future::BoxFuture<'a, Result<Value, ControlFlow>>;
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

// ============================================================================
// INSERT
// ============================================================================

/// A new node an [`InsertGraph`] creates.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertNodeOp<V = Clause> {
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
pub(crate) struct InsertEdgeOp<V = Clause> {
	pub(crate) name: String,
	pub(crate) table: TableName,
	pub(crate) from: String,
	pub(crate) to: String,
	/// The row-scoped property object, evaluated per row.
	pub(crate) props: V,
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
