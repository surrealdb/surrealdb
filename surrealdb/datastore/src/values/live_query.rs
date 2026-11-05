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
use uuid::Uuid;

use crate::values::save_point::{fold_frames, pop_save_point, reset_frames};

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
#[revisioned(revision = 2)]
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
	/// The versionstamp minted when the write was captured (`0` for entries
	/// written before revision 2). Checked alongside the commit versionstamp
	/// against each subscription's watermark so a write captured before a
	/// subscription was registered is not delivered to it even when its
	/// transaction commits after the registration.
	#[revision(start = 2)]
	pub captured: u128,
}

/// All live-query events for a single table within one committed transaction.
///
/// One entry per (table, commit), mirroring how the change feed groups per-table
/// mutations, so the write cost stays O(1) per modified table per transaction.
#[revisioned(revision = 2)]
#[derive(Clone, Debug, PartialEq)]
pub struct LiveEvents {
	/// The changes, in the order the transaction made them.
	pub events: Vec<LiveEvent>,
	/// The subscriptions the transaction registered on the table once it had
	/// made some of the changes, each with the number it had made by then.
	/// The commit stamps every change alike, so this is what tells the
	/// router which of them precede a subscription: it is notified of the
	/// changes after its registration alone, as the inline engine notifies
	/// it. Empty for a transaction that registered none, and for an entry
	/// written before the field existed.
	#[revision(start = 2)]
	pub registered: Vec<Registration>,
}

/// A subscription a transaction registered on a table after making `after`
/// of the changes its entry holds.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Registration {
	pub id: Uuid,
	pub after: u32,
}

impl_kv_value_revisioned!(LiveEvents);

impl Default for LiveEvents {
	fn default() -> Self {
		Self::new()
	}
}

impl LiveEvents {
	pub fn new() -> Self {
		Self {
			events: Vec::new(),
			registered: Vec::new(),
		}
	}

	/// Append a record change, deriving the action from the before/after values.
	///
	/// Matches the changefeed's classification: a nullish `after` is a delete, an
	/// absent `before` is a create, otherwise it is an update.
	pub fn push_record_change(
		&mut self,
		id: RecordId,
		before: Value,
		after: Value,
		captured: u128,
	) {
		let action = if after.is_nullish() {
			LiveAction::Delete
		} else if before.is_none() {
			LiveAction::Create
		} else {
			LiveAction::Update
		};
		self.events.push(LiveEvent {
			action,
			id,
			before,
			after,
			captured,
		});
	}

	/// Note that the subscription `id` was registered after the changes made
	/// so far, which the router then does not notify it of.
	pub fn note_registration(&mut self, id: Uuid) {
		let after = u32::try_from(self.events.len()).unwrap_or(u32::MAX);
		self.registered.push(Registration {
			id,
			after,
		});
	}

	/// Append `other`'s events and registrations after `self`'s, shifting each
	/// registration's prefix count by the events already in `self`.
	pub fn extend(&mut self, other: Self) {
		let base = u32::try_from(self.events.len()).unwrap_or(u32::MAX);
		self.events.extend(other.events);
		self.registered.extend(other.registered.into_iter().map(|reg| Registration {
			id: reg.id,
			after: reg.after.saturating_add(base),
		}));
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
	/// The save-point frame stack ([`crate::values::save_point`]). Every change
	/// is buffered into the top frame, so a rollback discards exactly the
	/// events the writes it undid produced.
	buffer: Mutex<Vec<HashMap<BufferKey, LiveEvents>>>,
}

impl Default for LiveEventBuffer {
	fn default() -> Self {
		Self::new()
	}
}

impl LiveEventBuffer {
	pub fn new() -> Self {
		Self {
			buffer: Mutex::new(vec![HashMap::new()]),
		}
	}

	/// Record a record modification or deletion for live-query routing.
	#[allow(clippy::too_many_arguments)]
	pub fn buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: RecordId,
		before: Value,
		after: Value,
		captured: u128,
	) {
		let mut frames = self.buffer.lock();
		let frame = frames.last_mut().expect("the transaction frame is always present");
		frame
			.entry(BufferKey {
				ns,
				db,
				tb: tb.clone(),
			})
			.or_default()
			.push_record_change(id, before, after, captured);
	}

	/// Note that the subscription `id` on `tb` was registered after the changes
	/// buffered for the table so far. Nothing is noted for a table with no
	/// change buffered: no change precedes the subscription then.
	pub fn note_registration(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName, id: Uuid) {
		let mut frames = self.buffer.lock();
		let key = BufferKey {
			ns,
			db,
			tb: tb.clone(),
		};
		if frames.iter().any(|frame| frame.get(&key).is_some_and(|e| !e.events.is_empty())) {
			let frame = frames.last_mut().expect("the transaction frame is always present");
			frame.entry(key).or_default().note_registration(id);
		}
	}

	/// Returns all buffered events as prepared writes. The commit versionstamp is
	/// added by the caller at flush time.
	///
	/// A scope still open here belongs to the transaction — the writes it
	/// describes are the ones about to be committed — so the stack is folded
	/// down before the events are read, which also restores the order they were
	/// buffered in.
	pub fn changes(&self) -> Result<Vec<PreparedWrite>> {
		let mut frames = self.buffer.lock();
		fold_frames(&mut frames, LiveEvents::extend);
		let buffer = frames.first().expect("the transaction frame is always present");
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
		reset_frames(&mut self.buffer.lock());
	}

	/// Open a save-point scope: the events buffered from here on are its own.
	pub fn push_save_point(&self) {
		self.buffer.lock().push(HashMap::new());
	}

	/// Fold the save point's frame into the one beneath it: its writes survived,
	/// so the events describing them belong to the enclosing scope, after
	/// whatever that scope had already buffered.
	pub fn release_save_point(&self) {
		let mut frames = self.buffer.lock();
		if let Some(top) = pop_save_point(&mut frames) {
			let parent = frames.last_mut().expect("popping left at least one frame");
			for (key, events) in top {
				match parent.get_mut(&key) {
					Some(entry) => entry.extend(events),
					None => {
						parent.insert(key, events);
					}
				}
			}
		}
	}

	/// Discard the save point's frame: the writes it described were undone, so a
	/// subscriber must never be told they happened.
	pub fn rollback_save_point(&self) {
		pop_save_point(&mut self.buffer.lock());
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(2);

	fn table() -> TableName {
		"t".into()
	}

	fn record(key: &str) -> RecordId {
		RecordId::new(table(), key.to_owned())
	}

	/// Buffer the creation of `t:key`.
	fn create(buffer: &LiveEventBuffer, key: &str) {
		buffer.buffer_record_change(
			NS,
			DB,
			&table(),
			record(key),
			Value::None,
			Value::from(key),
			0,
		);
	}

	/// The records the buffer would flush an event for, in the order it holds
	/// them — which is the order a subscriber replays them in.
	fn flushed(buffer: &LiveEventBuffer) -> Vec<RecordId> {
		buffer
			.changes()
			.unwrap()
			.into_iter()
			.flat_map(|(_, _, _, value)| LiveEvents::kv_decode_value(&value, ()).unwrap().events)
			.map(|event| event.id)
			.collect()
	}

	/// A rolled-back save point's writes are undone, so a subscriber must never
	/// be told they happened: the transaction still commits, and nothing later
	/// retracts a notification.
	#[test]
	fn a_rolled_back_save_point_delivers_none_of_its_events() {
		let buffer = LiveEventBuffer::new();
		create(&buffer, "kept");
		buffer.push_save_point();
		create(&buffer, "undone");
		buffer.rollback_save_point();
		assert_eq!(flushed(&buffer), vec![record("kept")]);
	}

	/// A released save point's writes survived, so its events belong to the
	/// enclosing scope, after whatever that scope had already buffered.
	#[test]
	fn a_released_save_point_hands_its_events_up_in_order() {
		let buffer = LiveEventBuffer::new();
		create(&buffer, "first");
		buffer.push_save_point();
		create(&buffer, "second");
		buffer.release_save_point();
		create(&buffer, "third");
		assert_eq!(flushed(&buffer), vec![record("first"), record("second"), record("third")]);
	}

	/// Nested, because a single scope cannot tell framing apart from one flat
	/// map: either discards the same events. What distinguishes them is an inner
	/// rollback under an outer scope that survives — the inner events have to go
	/// and the outer ones have to stay, which one map cannot do.
	#[test]
	fn nested_save_points_keep_only_the_events_that_survived() {
		let buffer = LiveEventBuffer::new();
		create(&buffer, "transaction");
		buffer.push_save_point();
		create(&buffer, "outer");
		buffer.push_save_point();
		create(&buffer, "inner");
		buffer.rollback_save_point();
		buffer.release_save_point();
		assert_eq!(flushed(&buffer), vec![record("transaction"), record("outer")]);
	}

	/// A scope still open at the flush belongs to the transaction: the writes it
	/// describes are the ones about to be committed.
	#[test]
	fn an_open_save_point_is_flushed_with_the_transaction() {
		let buffer = LiveEventBuffer::new();
		create(&buffer, "first");
		buffer.push_save_point();
		create(&buffer, "second");
		assert_eq!(flushed(&buffer), vec![record("first"), record("second")]);
	}

	/// Cancelling drops the open scopes along with their events, and leaves the
	/// stack at the floor depth every other operation assumes.
	#[test]
	fn clearing_drops_every_open_scope() {
		let buffer = LiveEventBuffer::new();
		create(&buffer, "a");
		buffer.push_save_point();
		create(&buffer, "b");
		buffer.clear();
		assert_eq!(buffer.buffer.lock().len(), 1);
		assert!(flushed(&buffer).is_empty());
	}

	/// A subscription registered inside a save point counts the surviving
	/// events that preceded it across enclosing frames, and a rolled-back save
	/// point drops the registrations made inside it along with its events.
	#[test]
	fn registrations_in_save_points_count_surviving_preceding_events() {
		let buffer = LiveEventBuffer::new();
		let discarded = Uuid::from_u128(1);
		let survived_before_child_write = Uuid::from_u128(2);
		let survived_after_child_write = Uuid::from_u128(3);

		create(&buffer, "first");
		buffer.push_save_point();
		create(&buffer, "undone");
		buffer.note_registration(NS, DB, &table(), discarded);
		buffer.rollback_save_point();

		buffer.push_save_point();
		buffer.note_registration(NS, DB, &table(), survived_before_child_write);
		create(&buffer, "second");
		buffer.note_registration(NS, DB, &table(), survived_after_child_write);
		buffer.release_save_point();
		create(&buffer, "third");

		let writes = buffer.changes().unwrap();
		assert_eq!(writes.len(), 1);
		let decoded = LiveEvents::kv_decode_value(&writes[0].3, ()).unwrap();
		let ids: Vec<RecordId> = decoded.events.into_iter().map(|e| e.id).collect();
		assert_eq!(ids, vec![record("first"), record("second"), record("third")]);
		assert_eq!(
			decoded.registered,
			vec![
				Registration {
					id: survived_before_child_write,
					after: 1,
				},
				Registration {
					id: survived_after_child_write,
					after: 2,
				},
			]
		);
	}
}
