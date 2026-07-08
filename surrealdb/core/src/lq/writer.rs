use std::collections::HashMap;

use anyhow::Result;
use parking_lot::Mutex;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::KVValue;
use crate::kvs::Val;
use crate::lq::event::LiveEvents;
use crate::val::{RecordId, TableName, Value};

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

/// Per-transaction buffer of live-query events, flushed to the dedicated
/// [`crate::key::lqe`] keyspace at commit time.
///
/// This deliberately mirrors [`crate::cf::Changefeed`] but is a separate buffer
/// writing a separate keyspace with a purpose-built value, so live queries and
/// changefeeds share no semantics (`store_diff`, retention, visibility) and only
/// the lower-level versionstamp/commit plumbing is reused.
pub(crate) struct LiveEventBuffer {
	buffer: Mutex<HashMap<BufferKey, LiveEvents>>,
}

impl LiveEventBuffer {
	pub(crate) fn new() -> Self {
		Self {
			buffer: Mutex::new(HashMap::new()),
		}
	}

	/// Record a record modification or deletion for live-query routing.
	pub(crate) fn buffer_record_change(
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
			.or_insert_with(LiveEvents::new)
			.push_record_change(id, before, after);
	}

	/// Returns all buffered events as prepared writes. The commit versionstamp is
	/// added by the caller at flush time.
	pub(crate) fn changes(&self) -> Result<Vec<PreparedWrite>> {
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
	pub(crate) fn clear(&self) {
		self.buffer.lock().clear();
	}
}
