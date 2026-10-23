//! A deleted record's storage, taken back out.
//!
//! One stage, and four things it does in this order: the record itself, the record's own inline
//! cache keys, the four graph keys an edge's endpoints hold, and the two cascades — over the
//! records joined to this one by an edge, and over the records that reference
//! it by a `REFERENCE` field.
//!
//! # Which of the row's two markers is read
//!
//! Every dispatch here reads
//! [`WriteRow::initial_metadata`](super::super::row::WriteRow), never
//! `metadata`. The keys being removed were written under the layout the record
//! *had*, and `ClearRecordData` — which runs earlier in `DELETE`'s order —
//! has already blanked the post-image and its marker. Reading `metadata` would
//! find `None`, take the latest-layout branch, and leave a legacy-layout edge's
//! four keys on disk with nothing left to name them.

use std::borrow::Cow;
use std::sync::Arc;

use super::store::table_is_lightweight;
use crate::doc::Error as DocError;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::expr::data::Assignment;
use crate::expr::logical::{LogicalOp, LogicalSubquery, lower_select};
use crate::expr::lookup::LookupKind;
use crate::expr::statements::{DeleteStatement, SelectStatement, UpdateStatement};
use crate::expr::{
	AssignOperator, ControlFlow, Data, Dir, Expr, Field, Fields, Idiom, Literal, Lookup, Param,
	Part,
};
use crate::idx::adjacency::{AdjacencyScope, MergedAdjacencyCursor};
use crate::key::KVKeyDecode;
use crate::key::schema::{GraphIdPrefix, ReferenceIdPrefix, ReferenceKey};
use crate::kvs::{Direction, NORMAL_BATCH_SIZE, Transaction};
use crate::val::RecordId;

// ===========================================================================
// The cascades
// ===========================================================================

/// `$__purge_vertex`: the record whose graph edges are being deleted.
pub(crate) const PURGE_VERTEX: &str = "__purge_vertex";
/// `$__purge_referencing`: the record holding a `REFERENCE` field that named
/// the record being deleted.
pub(crate) const PURGE_REFERENCING: &str = "__purge_referencing";
/// `$__purge_deleted`: the record being deleted, as an `ON DELETE UNSET`
/// repair of a collection subtracts it.
pub(crate) const PURGE_DELETED: &str = "__purge_deleted";
/// `$reference`: the record being deleted, as a custom delete strategy names
/// it. The name is the user-facing one the strategy is written against.
pub(crate) const REFERENCE: &str = "reference";

/// `DELETE $__purge_vertex<->`, as a plan.
pub(crate) fn purge_edges_statement() -> Result<LogicalOp, ControlFlow> {
	let what = Expr::Idiom(Idiom(vec![
		Part::Start(Expr::Param(Param::from(PURGE_VERTEX.to_owned()))),
		Part::Lookup(Box::new(Lookup {
			kind: LookupKind::Graph(Dir::Both),
			..Default::default()
		})),
	]));
	delete_of(what)
}

/// Whether the record `rid`, of the table `ctx` writes, has an edge to
/// cascade over.
///
/// A lightweight edge has none: nothing can connect to it, since it is
/// refused as a graph endpoint, and its synthesized adjacency derives from its
/// id whether or not it exists, so the peek would see its own endpoints.
pub(crate) async fn has_edges(ctx: &WriteCtx, rid: &RecordId) -> Result<bool, ControlFlow> {
	if table_is_lightweight(&ctx.table) {
		return Ok(false);
	}
	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let txn = ctx.txn();
	let delta_range = GraphIdPrefix {
		ns,
		db,
		tb: Cow::Borrowed(&rid.table),
		id: Cow::Borrowed(&rid.key),
	}
	.range()
	.map_err(ControlFlow::Err)?;
	let mut cursor = MergedAdjacencyCursor::open(
		&txn,
		AdjacencyScope {
			ns,
			db,
			vertex: rid,
			dir: None,
			edge_table: None,
			fk_lower: None,
			delta_range,
		},
		ctx.table.graph_folded,
		None,
		Some(ctx.exec().root().ctx.get_index_stores().adjacency_resolve()),
	)
	.await
	.map_err(ControlFlow::Err)?;
	// Only emptiness matters, so one key is fetched and nothing is decoded.
	let any = !cursor.next_batch_keys(1).await.map_err(ControlFlow::Err)?.is_empty();
	drop(cursor);
	Ok(any)
}

#[cfg(test)]
thread_local! {
	/// How many times this thread dispatched a purge's reference repairs through
	/// [`purge_references`], for tests that must know a delete dispatched them as
	/// instructions.
	pub(crate) static CASCADE_DISPATCHES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The reference keys naming `rid`, drained, and the range they were drained
/// from; `None` when no field in the database can target `rid`'s table, or
/// none names `rid`. See [`purge_references`] for why the whole range is read
/// before any repair.
pub(crate) async fn referencing_keys(
	ctx: &WriteCtx,
	rid: &RecordId,
) -> Result<Option<(surrealdb_kvs::key::TypedRange<()>, Vec<ReferenceKey<'static>>)>, ControlFlow> {
	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let txn = ctx.txn();
	if !txn
		.table_may_have_incoming_references(ns, db, &rid.table)
		.await
		.map_err(ControlFlow::Err)?
	{
		return Ok(None);
	}
	let range = ReferenceIdPrefix {
		ns,
		db,
		tb: Cow::Borrowed(&rid.table),
		id: Cow::Borrowed(&rid.key),
	}
	.range()
	.map_err(ControlFlow::Err)?;
	let keys = drain_references(&txn, range.clone()).await?;
	// An empty range leaves nothing to repay, and the range delete would still
	// cost the transaction an op.
	if keys.is_empty() {
		return Ok(None);
	}
	Ok(Some((range, keys)))
}

/// Read a whole reference range into owned keys, with the cursor dropped
/// before the caller does anything else; see [`purge_references`].
async fn drain_references(
	txn: &Arc<Transaction>,
	range: surrealdb_kvs::key::TypedRange<()>,
) -> Result<Vec<ReferenceKey<'static>>, ControlFlow> {
	let mut cursor =
		txn.open_keys_cursor(range, Direction::Forward, 0, None).await.map_err(ControlFlow::Err)?;
	let mut keys = Vec::new();
	loop {
		let batch = cursor.next_batch(NORMAL_BATCH_SIZE).await.map_err(ControlFlow::Err)?;
		if batch.is_empty() {
			return Ok(keys);
		}
		for encoded in batch.iter() {
			keys.push(
				ReferenceKey::decode_key(encoded)
					.map(|key| key.into_owned())
					.map_err(ControlFlow::Err)?,
			);
		}
	}
}

/// The data of the update that takes a link out of the single-link field
/// `field`: `UNSET field`.
pub(crate) fn unset_data(field: &Idiom) -> Data {
	Data::UnsetExpression(vec![field.clone()])
}

/// The data of the update that takes the deleted record out of the collection
/// field `field`: `SET field -= $__purge_deleted`.
pub(crate) fn subtract_data(field: &Idiom) -> Data {
	Data::SetExpression(vec![Assignment {
		place: field.clone(),
		operator: AssignOperator::Subtract,
		value: Expr::Param(Param::from(PURGE_DELETED.to_owned())),
	}])
}

/// Report a failed repair as the referential failure it is. The record whose
/// deletion triggered it is named by `rid`, already reduced to the display the
/// caller is owed: `Some(id)` when the caller may read that record, and `None`
/// to withhold it so a record a permission-checked caller cannot read is not
/// disclosed in the wrapped message.
pub(crate) fn wrap(rid: Option<String>, err: ControlFlow) -> ControlFlow {
	err.map_err(|err| anyhow::anyhow!(DocError::RefsUpdateFailure(rid, err.to_string())))
}

// ===========================================================================
// The statements, built rather than parsed
// ===========================================================================

/// `DELETE <what>`, as a plan.
pub(crate) fn delete_of(what: Expr) -> Result<LogicalOp, ControlFlow> {
	let statement = DeleteStatement {
		what: vec![what],
		..Default::default()
	};
	lowered(Expr::Delete(Box::new(statement)))
}

/// `UPDATE <what> <data>`, as a plan.
pub(crate) fn update_of(what: Expr, data: Data) -> Result<LogicalOp, ControlFlow> {
	let statement = UpdateStatement {
		what: vec![what],
		data: Some(data),
		..Default::default()
	};
	lowered(Expr::Update(Box::new(statement)))
}

/// `SELECT * FROM <what>`, as a plan.
pub(crate) fn select_all_of(what: Expr) -> LogicalOp {
	let statement = SelectStatement {
		fields: Fields::Select(vec![Field::All]),
		what: vec![what],
		omit: vec![],
		only: false,
		with: None,
		cond: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	};
	LogicalOp::Subquery(LogicalSubquery {
		input: Box::new(
			lower_select(statement).expect("a SELECT of all fields has a logical form"),
		),
	})
}

/// One built statement, lowered.
///
/// The statements above are assembled as the engine's own AST and converted,
/// rather than formatted and re-parsed: a field name reaches them as the
/// catalog holds it, and nothing depends on that name surviving a round trip
/// through text.
fn lowered(statement: Expr) -> Result<LogicalOp, ControlFlow> {
	crate::expr::logical::lower(statement).map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))
}
