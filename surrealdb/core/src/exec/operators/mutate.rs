//! The row handling the VM's GQL mutation steps share: `SET`/`REMOVE`
//! ([`update_row`]) and `DELETE` ([`delete_row`]) over a binding table, and
//! `INSERT` ([`insert_row`]), generic over how a row-scoped expression is
//! evaluated ([`RowEval`]) and how a write is run ([`RunWrite`]).
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
//! `RelateStatement` ([`update_write`], [`delete_write`], [`create_write`],
//! [`relate_write`]) targeting the resolved record. The statement is bound with
//! the `MATCH` that holds the step and lowered into a group of the step's
//! program, so table/field permissions, field validation, events, indexes,
//! references, and live-query notifications apply exactly as for that
//! statement. A step runs its group once per row ([`RunWrite`]), in the
//! step's execution context, with the row's record and values bound to the
//! parameters the statement reads ([`MutationParam`]). The row-scoped values a
//! step writes are evaluated against the current binding row before the group
//! runs.

use std::borrow::Cow;

use surrealdb_types::ToSql;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::exec::ExecutionContext;
use crate::exec::ir::iface::statements::MatchData;
use crate::expr::data::Assignment;
use crate::expr::logical::LogicalOp;
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
pub(crate) enum UpdateOp<V> {
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

/// The prefix of every parameter name a mutation's write reads its per-row
/// values from. The names are internal: the parsers reject them in user text,
/// and the planning catalog reads them at run time, never folding them.
pub(crate) const MUTATION_PARAM_PREFIX: &str = "__mutation_";

/// The names a mutation's synthetic statement reads its per-row values from.
#[derive(Clone, Copy)]
pub(crate) enum MutationParam {
	Record,
	From,
	To,
	Data,
	Value(usize),
}

impl MutationParam {
	pub(crate) fn name(self) -> String {
		match self {
			MutationParam::Record => format!("{MUTATION_PARAM_PREFIX}record"),
			MutationParam::From => format!("{MUTATION_PARAM_PREFIX}from"),
			MutationParam::To => format!("{MUTATION_PARAM_PREFIX}to"),
			MutationParam::Data => format!("{MUTATION_PARAM_PREFIX}data"),
			MutationParam::Value(i) => format!("{MUTATION_PARAM_PREFIX}value_{i}"),
		}
	}

	pub(crate) fn expr(self) -> Expr {
		Expr::Param(self.name().into())
	}
}

/// Lowers `write`, a statement a mutation applies to one record, to the plan
/// the binder binds.
fn lowered(write: Expr) -> Result<LogicalOp, crate::exec::Error> {
	crate::expr::logical::lower(write).map_err(|e| {
		crate::exec::Error::InvalidStatement(format!("a mutation's write did not lower: {e}"))
	})
}

/// The `UPDATE` of the record in [`MutationParam::Record`] a `SET`, `REMOVE` or
/// `SET a = {…}` step applies, answering the record's `AFTER` image. A `SET`
/// assignment `i` reads its value from [`MutationParam::Value`]`(i)`, and a
/// content update reads its object from [`MutationParam::Data`].
pub(crate) fn update_write(data: &MatchData) -> Result<LogicalOp, crate::exec::Error> {
	let data = match data {
		MatchData::Set(names) => Data::SetExpression(
			names
				.iter()
				.enumerate()
				.map(|(i, name)| Assignment {
					place: Idiom::field(name.clone()),
					operator: AssignOperator::Assign,
					value: MutationParam::Value(i).expr(),
				})
				.collect(),
		),
		MatchData::Unset(names) => {
			Data::UnsetExpression(names.iter().map(|name| Idiom::field(name.clone())).collect())
		}
		MatchData::Content => Data::ContentExpression(MutationParam::Data.expr()),
	};
	let stmt = UpdateStatement {
		what: vec![MutationParam::Record.expr()],
		data: Some(data),
		output: Some(Output::After),
		..Default::default()
	};
	lowered(Expr::Update(Box::new(stmt)))
}

/// The `DELETE` of the record in [`MutationParam::Record`].
pub(crate) fn delete_write() -> Result<LogicalOp, crate::exec::Error> {
	let stmt = DeleteStatement {
		what: vec![MutationParam::Record.expr()],
		..Default::default()
	};
	lowered(Expr::Delete(Box::new(stmt)))
}

/// The `CREATE` of a record of `table` with the content of
/// [`MutationParam::Data`], answering its `AFTER` image.
pub(crate) fn create_write(table: &TableName) -> Result<LogicalOp, crate::exec::Error> {
	let stmt = CreateStatement {
		what: vec![Expr::Table(table.clone())],
		data: Some(Data::ContentExpression(MutationParam::Data.expr())),
		output: Some(Output::After),
		..Default::default()
	};
	lowered(Expr::Create(Box::new(stmt)))
}

/// The `RELATE` of the record in [`MutationParam::From`] to the one in
/// [`MutationParam::To`] through `table`, with the content of
/// [`MutationParam::Data`], answering the edge's `AFTER` image.
pub(crate) fn relate_write(table: &TableName) -> Result<LogicalOp, crate::exec::Error> {
	let stmt = RelateStatement {
		only: false,
		or_update: false,
		through: Expr::Table(table.clone()),
		from: MutationParam::From.expr(),
		to: MutationParam::To.expr(),
		data: Some(Data::ContentExpression(MutationParam::Data.expr())),
		output: Some(Output::After),
		timeout: Expr::Literal(Literal::None),
	};
	lowered(Expr::Relate(Box::new(stmt)))
}

/// Runs the writes a mutation step applies.
pub(crate) trait RunWrite {
	/// Runs write group `write` of the running program with `values` bound to the
	/// parameters it reads, and answers its rows.
	fn run<'a>(
		&'a mut self,
		write: u16,
		values: Vec<(MutationParam, Value)>,
	) -> futures::future::BoxFuture<'a, Result<Vec<Value>, ControlFlow>>;
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

/// Apply `op` to the record bound at `target` in `row` by running write group
/// `write`, rebinding `row[target]` to the record's AFTER image. A null /
/// unresolvable target binding is passed through untouched.
pub(crate) async fn update_row<V>(
	op: &UpdateOp<V>,
	write: u16,
	target: &str,
	row: &mut Value,
	eval: &mut impl RowEval<V>,
	writes: &mut impl RunWrite,
) -> Result<(), ControlFlow> {
	if let Some(rid) = target_record_id(row, target) {
		let after = apply_update(op, write, &rid, row, eval, writes).await?;
		set_binding(row, target, after);
	}
	Ok(())
}

/// Delete the record bound at `target` in `row` by running write group `write`,
/// once per distinct record in `deleted`, and rebind it to `NULL`, as every edge
/// of the row incident to it. `(ns, db)` are the ids of the database the
/// connected-edge probe reads.
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
	write: u16,
	writes: &mut impl RunWrite,
	ctx: &ExecutionContext,
) -> Result<(), ControlFlow> {
	if let Some(rid) = target_record_id(row, target) {
		if deleted.insert(rid.clone()) {
			apply_delete(&rid, detach, is_edge, ns, db, write, writes, ctx).await?;
		}
		// The record is gone; surface NULL for it, and for any other binding in
		// this row that is an edge incident to it (a `DETACH` delete cascades
		// those edges, so they no longer exist).
		set_binding(row, target, Value::Null);
		null_incident_edges(row, &rid);
	}
	Ok(())
}

/// Evaluate the values of the update of one resolved record, run write group
/// `write` with them, and return the record's AFTER image (or `Value::Null`).
async fn apply_update<V>(
	op: &UpdateOp<V>,
	write: u16,
	rid: &RecordId,
	row: &Value,
	eval: &mut impl RowEval<V>,
	writes: &mut impl RunWrite,
) -> Result<Value, ControlFlow> {
	let mut values = vec![(MutationParam::Record, Value::RecordId(rid.clone()))];
	match op {
		UpdateOp::Set(assignments) => {
			// Every assignment in one `SET` evaluates against the same pre-write
			// row image.
			for (i, (_, value)) in assignments.iter().enumerate() {
				values.push((MutationParam::Value(i), eval.eval(value, row).await?));
			}
		}
		UpdateOp::Unset(_) => {}
		UpdateOp::Content(object) => {
			values.push((MutationParam::Data, eval.eval(object, row).await?));
		}
	}
	Ok(first_row(writes.run(write, values).await?))
}

/// Run write group `write` for one resolved record. `NoDetach` (the ISO
/// default) first errors if the node still has connected edges; `Detach`
/// cascades them (native `DELETE` always purges connected edges).
#[expect(
	clippy::too_many_arguments,
	reason = "the record and the step's mode, the ids of its database, and the write to run"
)]
async fn apply_delete(
	rid: &RecordId,
	detach: DetachMode,
	is_edge: bool,
	ns: NamespaceId,
	db: DatabaseId,
	write: u16,
	writes: &mut impl RunWrite,
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
	writes.run(write, vec![(MutationParam::Record, Value::RecordId(rid.clone()))]).await?;
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

/// The record a mutation's write answers: the first row of its group, or
/// `Value::Null` when the write answered none.
fn first_row(rows: Vec<Value>) -> Value {
	rows.into_iter().next().unwrap_or(Value::Null)
}

// ============================================================================
// INSERT
// ============================================================================

/// A new node an [`InsertGraph`] creates.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertNodeOp<V> {
	/// The binding name the created record is bound under.
	pub(crate) name: String,
	/// The target table (= label).
	pub(crate) table: TableName,
	/// The row-scoped property object, evaluated per row.
	pub(crate) props: V,
	/// The write group that creates the node.
	pub(crate) write: u16,
}

/// A new edge an [`InsertGraph`] relates between two endpoints (each a node
/// created by this stage or a read-bound reference), addressed by binding name.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InsertEdgeOp<V> {
	pub(crate) name: String,
	pub(crate) table: TableName,
	pub(crate) from: String,
	pub(crate) to: String,
	/// The row-scoped property object, evaluated per row.
	pub(crate) props: V,
	/// The write group that relates the edge.
	pub(crate) write: u16,
}

/// Create the new nodes (binding each into the row so later nodes/edges can
/// reference it) and relate the edges, for one input row.
pub(crate) async fn insert_row<V>(
	nodes: &[InsertNodeOp<V>],
	edges: &[InsertEdgeOp<V>],
	row: &mut Value,
	eval: &mut impl RowEval<V>,
	writes: &mut impl RunWrite,
) -> Result<(), ControlFlow> {
	for node in nodes {
		let props = eval.eval(&node.props, row).await?;
		let rows = writes.run(node.write, vec![(MutationParam::Data, props)]).await?;
		set_binding(row, &node.name, first_row(rows));
	}
	for edge in edges {
		let from = target_record_id(row, &edge.from).ok_or_else(|| endpoint_error(&edge.from))?;
		let to = target_record_id(row, &edge.to).ok_or_else(|| endpoint_error(&edge.to))?;
		let props = eval.eval(&edge.props, row).await?;
		let values = vec![
			(MutationParam::From, Value::RecordId(from)),
			(MutationParam::To, Value::RecordId(to)),
			(MutationParam::Data, props),
		];
		let rows = writes.run(edge.write, values).await?;
		set_binding(row, &edge.name, first_row(rows));
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
