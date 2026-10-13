//! What the live-query write path captures for the router to replay.
//!
//! Under the router engine a write does not evaluate any subscriber's query. It
//! records the record's before and after values into the `%` section of the
//! keyspace and returns; the per-node router reads them back off the write path
//! and does the matching there. These are the two records that make that handoff
//! durable: the event itself, and the per-transaction buffer that batches one
//! entry per modified table and hands it to the commit.
//!
//! Unlike the change feed, an event always retains its before value, independent
//! of any user `CHANGEFEED ... [INCLUDE ORIGINAL]` setting: the router needs both
//! sides to evaluate WHERE clauses, permissions, projections and DIFFs.
//!
//! The router and the subscriber-side matching read these downward from the layer
//! above; they need the engine's document machinery, so they cannot descend.

use std::collections::HashMap;

use anyhow::Result;
use parking_lot::Mutex;
use revision::revisioned;
use surrealdb_catalog::{DatabaseId, NamespaceId};
use surrealdb_expr::val::{RecordId, TableName, Value};
use surrealdb_kvs::value::KVValue;
use surrealdb_kvs::{Val, impl_kv_value_revisioned};

/// The kind of mutation that produced a [`LiveEvent`].
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum LiveAction {
	Create,
	Update,
	Delete,
}

/// A single record change captured for live-query routing.
///
/// Unlike a change feed mutation, this carries
/// the **full** before and after document values directly (no reverse-patch
/// encoding). The router needs both to evaluate WHERE clauses, permissions,
/// projections, and DIFFs on the subscriber side, so the simplest, most useful
/// representation is to store them outright. This is the deliberate decoupling
/// from the changefeed format: live queries always retain before-values,
/// independent of any user `CHANGEFEED ... [INCLUDE ORIGINAL]` setting.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub struct LiveEvent {
	/// The mutation kind.
	pub action: LiveAction,
	/// The record that changed.
	pub id: RecordId,
	/// The document before the change (`Value::None` for a create).
	pub before: Value,
	/// The document after the change (`Value::None` for a delete).
	pub after: Value,
}

/// All live-query events for a single table within one committed transaction.
///
/// One entry per (table, commit), mirroring how the change feed groups per-table
/// mutations, so the write cost stays O(1) per modified table per transaction.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub struct LiveEvents(pub Vec<LiveEvent>);

impl_kv_value_revisioned!(LiveEvents);

impl Default for LiveEvents {
	fn default() -> Self {
		Self::new()
	}
}

impl LiveEvents {
	pub fn new() -> Self {
		Self(Vec::new())
	}

	/// Append a record change, deriving the action from the before/after values.
	///
	/// Matches the changefeed's classification: a nullish `after` is a delete, an
	/// absent `before` is a create, otherwise it is an update.
	pub fn push_record_change(&mut self, id: RecordId, before: Value, after: Value) {
		let action = if after.is_nullish() {
			LiveAction::Delete
		} else if before.is_none() {
			LiveAction::Create
		} else {
			LiveAction::Update
		};
		self.0.push(LiveEvent {
			action,
			id,
			before,
			after,
		});
	}
}

/// A prepared write: (namespace, database, table, serialized [`LiveEvents`]).
/// The commit versionstamp is applied at flush time, identically to the
/// changefeed writer.
type PreparedWrite = (NamespaceId, DatabaseId, TableName, Val);

/// Identifies the per-table bucket within a transaction's live-event buffer.
#[derive(Hash, Eq, PartialEq, Debug)]
struct BufferKey {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
}

/// Per-transaction buffer of live-query events, flushed to the `%` section of the
/// keyspace at commit time.
///
/// This deliberately mirrors the change feed's buffer but is a separate one
/// writing a separate keyspace with a purpose-built value, so live queries and
/// changefeeds share no semantics (`store_diff`, retention, visibility) and only
/// the lower-level versionstamp/commit plumbing is reused.
pub struct LiveEventBuffer {
	buffer: Mutex<HashMap<BufferKey, LiveEvents>>,
}

impl Default for LiveEventBuffer {
	fn default() -> Self {
		Self::new()
	}
}

impl LiveEventBuffer {
	pub fn new() -> Self {
		Self {
			buffer: Mutex::new(HashMap::new()),
		}
	}

	/// Record a record modification or deletion for live-query routing.
	pub fn buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: RecordId,
		before: Value,
		after: Value,
	) {
		let mut buffer = self.buffer.lock();
		buffer
			.entry(BufferKey {
				ns,
				db,
				tb: tb.clone(),
			})
			.or_default()
			.push_record_change(id, before, after);
	}

	/// Returns all buffered events as prepared writes. The commit versionstamp is
	/// added by the caller at flush time.
	pub fn changes(&self) -> Result<Vec<PreparedWrite>> {
		let buffer = self.buffer.lock();
		if buffer.is_empty() {
			return Ok(Vec::new());
		}
		let mut res = Vec::with_capacity(buffer.len());
		for (key, events) in buffer.iter() {
			let value = events.kv_encode_value()?;
			res.push((key.ns, key.db, key.tb.clone(), value));
		}
		Ok(res)
	}

	/// Clear the buffer (used on transaction cancel/rollback).
	pub fn clear(&self) {
		self.buffer.lock().clear();
	}
}
