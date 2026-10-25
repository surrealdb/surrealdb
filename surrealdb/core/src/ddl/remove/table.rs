//! What `REMOVE TABLE` takes out beyond the table's own keyspace.
//!
//! An edge is stored as its record, under its own table, and as a pointer on
//! each endpoint, under the endpoint's table: a `~` key naming the edge on its
//! `in` record in direction out and on its `out` record in direction in, held
//! in a packed `^` block once the endpoint's table folds adjacency, and
//! mirrored in each endpoint's inline edge cache. Removing the table deletes
//! its own keyspace, which holds none of those, so the removal purges them
//! first. Without the purge a traversal from an endpoint keeps reaching the
//! removed edges, through the target a pointer key carries, and a table defined
//! again under the name finds them beside its own.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::catalog::providers::TableProvider as _;
use crate::catalog::{DatabaseId, NamespaceId, Record, StoredRecord, TableLayouts};
use crate::exec::ExecutionContext;
use crate::expr::paths::{IN, OUT};
use crate::expr::{ControlFlow, Dir};
use crate::key::schema::{
	AdjacencyEdgeTablePrefix, GraphForeignTablePrefix, GraphKey, GraphPrefix, RecordKey,
	RecordPrefix,
};
use crate::key::{AnyRange, KVKeyDecode, KVSubspace};
use crate::kvs::{NORMAL_BATCH_SIZE, Transaction};
use crate::val::{RecordId, TableName, Value};

/// One edge of the table being removed: its id and the endpoints it was
/// written under.
struct Edge {
	id: RecordId,
	from: RecordId,
	to: RecordId,
}

/// Purges the vertex-side adjacency of every edge `table` holds, as `txn`
/// holds them, before the table's own keyspace is deleted.
///
/// Each endpoint loses the `~` keys naming the edge, in the legacy layout and
/// the pointer layout alike, and the edge leaves its inline edge cache. On an
/// endpoint whose table folds adjacency the edge may also sit in a packed
/// block, which no key deletion reaches and which can resolve the edge only
/// through the removed table's doc-id space: there the endpoint's whole scope
/// for `table`, in that direction, is deleted, blocks and keys alike, once per
/// scope. Every edge in such a scope is an edge of `table`, so nothing else is
/// lost. Under `expunge` every earlier version of each key goes too.
///
/// A table holds an edge only if its own `~` range holds a key, since every
/// non-lightweight edge writes its edge-side keys there and no fold moves them,
/// so a table whose range is empty is not read further. The same range holds
/// the pointers of the edges that reach the table's own records, and no key
/// tells the two apart, so the records of any table an edge touches are read;
/// a record that is no edge has only its envelope's header parsed. The table's
/// type does not bound the read: a table written by an earlier release can
/// hold edges its type no longer admits. A lightweight relation writes no
/// edge-side key, and is removed only while it holds no edges.
///
/// The records are read in pages sized to the fuel left, a unit charged per
/// record, so the purge honours the statement's cancellation and deadline. A
/// page is read in full before its writes, so no cursor is held across them.
pub(crate) async fn purge_edge_pointers(
	exec: &ExecutionContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	expunge: bool,
) -> Result<()> {
	for fd in txn.all_tb_fields(ns, db, table, None).await?.iter() {
		crate::exec::reference_purge::purge_dropped_reference_keys_with(
			txn, ns, db, table, fd, None, expunge,
		)
		.await?;
	}
	let graph = GraphPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
	};
	if txn.keys_raw(graph.range()?, 1, 0, None).await?.is_empty() {
		return Ok(());
	}
	let layouts = txn.table_layouts(ns, db, table, None).await?;
	let records = RecordPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
	};
	let mut purge = Purge {
		txn,
		ns,
		db,
		table,
		expunge,
		folded: HashMap::new(),
		wiped: HashSet::new(),
	};
	let mut next = Some(records.range()?);
	while let Some(range) = next {
		let size = crate::exec::vm::fuel::batch(NORMAL_BATCH_SIZE);
		let page = txn.batch_keys_vals_raw(range, size, None).await?;
		next = page.next.map(|range| records.raw(range));
		if page.result.is_empty() {
			break;
		}
		let read = page.result.len();
		for (key, value) in &page.result {
			if let Some(edge) = edge_of(table, key, value, &layouts)? {
				purge.edge(&edge).await?;
			}
		}
		crate::exec::vm::fuel::tick(exec, crate::exec::vm::fuel::units(read))
			.await
			.map_err(interrupted)?;
	}
	Ok(())
}

/// The edge a stored record of `table` is, or `None` for a record that is no
/// edge or names an endpoint that is no record id. Whether the record is an
/// edge is read off its envelope's header where it has one, so a record that
/// is not is never decoded.
fn edge_of(
	table: &TableName,
	key: &[u8],
	value: &[u8],
	layouts: &TableLayouts,
) -> Result<Option<Edge>> {
	if StoredRecord::parse(value)?.is_some_and(|stored| !stored.is_edge()) {
		return Ok(None);
	}
	let id = RecordId {
		table: table.clone(),
		key: RecordKey::decode_key(key)?.id.into_owned(),
	};
	let record = Record::decode_stored(value, Some(layouts), id.clone())?;
	if !record.is_edge() {
		return Ok(None);
	}
	let (Value::RecordId(from), Value::RecordId(to)) =
		(record.data.pick(&IN), record.data.pick(&OUT))
	else {
		return Ok(None);
	};
	Ok(Some(Edge {
		id,
		from,
		to,
	}))
}

/// The purge of one table's edges, and what it has learned of the endpoint
/// tables along the way.
struct Purge<'a> {
	txn: &'a Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &'a TableName,
	expunge: bool,
	/// Whether each endpoint table met folds adjacency.
	folded: HashMap<TableName, bool>,
	/// The folded scopes already deleted whole.
	wiped: HashSet<(RecordId, Dir)>,
}

impl Purge<'_> {
	/// Removes `edge` from its `in` record's outgoing adjacency and its `out`
	/// record's incoming adjacency.
	async fn edge(&mut self, edge: &Edge) -> Result<()> {
		self.endpoint(&edge.from, Dir::Out, &edge.id).await?;
		self.endpoint(&edge.to, Dir::In, &edge.id).await
	}

	/// Removes `edge` from `vertex`'s adjacency in direction `dir`.
	async fn endpoint(&mut self, vertex: &RecordId, dir: Dir, edge: &RecordId) -> Result<()> {
		let (ns, db) = (self.ns, self.db);
		if self.is_folded(&vertex.table).await? {
			if self.wiped.insert((vertex.clone(), dir)) {
				let tb = Cow::Borrowed(&vertex.table);
				let id = Cow::Borrowed(&vertex.key);
				let edge_table = Cow::Borrowed(self.table);
				let keys = GraphForeignTablePrefix {
					ns,
					db,
					tb: tb.clone(),
					id: id.clone(),
					dir,
					foreign_table: edge_table.clone(),
				};
				let blocks = AdjacencyEdgeTablePrefix {
					ns,
					db,
					tb,
					id,
					dir,
					edge_table,
				};
				self.delete(keys.range()?).await?;
				self.delete(blocks.range()?).await?;
			}
		} else {
			// The inner key of the legacy layout and every pointer key built on it.
			let keys = GraphKey::new(
				ns,
				db,
				Cow::Borrowed(&vertex.table),
				Cow::Borrowed(&vertex.key),
				dir,
				Cow::Borrowed(&edge.table),
				Cow::Borrowed(&edge.key),
			);
			self.delete(keys.range_subtree()?).await?;
		}
		crate::idx::inline_cache::record_edge_delete(self.txn, ns, db, vertex, dir, edge).await
	}

	/// Whether `table` folds adjacency, read once per table.
	async fn is_folded(&mut self, table: &TableName) -> Result<bool> {
		if let Some(folded) = self.folded.get(table) {
			return Ok(*folded);
		}
		let folded =
			crate::idx::adjacency::vertex_table_folded(self.txn, self.ns, self.db, table).await?;
		self.folded.insert(table.clone(), folded);
		Ok(folded)
	}

	/// Deletes every key in `range`, and under `EXPUNGE` every earlier version
	/// of each.
	async fn delete(&self, range: impl AnyRange) -> Result<()> {
		if self.expunge {
			self.txn.clrr(range).await
		} else {
			self.txn.delr(range).await
		}
	}
}

/// The error a purge interrupted by its statement's cancellation or deadline
/// reports.
fn interrupted(flow: ControlFlow) -> anyhow::Error {
	flow.into_error_or(|other| {
		anyhow::anyhow!("the purge of the removed table's edges was interrupted by {other}")
	})
}
