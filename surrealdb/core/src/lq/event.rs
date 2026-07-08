use revision::revisioned;

use crate::key::impl_kv_value_revisioned;
use crate::val::{RecordId, Value};

/// The kind of mutation that produced a [`LiveEvent`].
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum LiveAction {
	Create,
	Update,
	Delete,
}

/// A single record change captured for live-query routing.
///
/// Unlike a changefeed [`TableMutation`](crate::cf::TableMutation), this carries
/// the **full** before and after document values directly (no reverse-patch
/// encoding). The router needs both to evaluate WHERE clauses, permissions,
/// projections, and DIFFs on the subscriber side, so the simplest, most useful
/// representation is to store them outright. This is the deliberate decoupling
/// from the changefeed format: live queries always retain before-values,
/// independent of any user `CHANGEFEED ... [INCLUDE ORIGINAL]` setting.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LiveEvent {
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
/// This is the value stored at each [`crate::key::lqe`] key — one entry per
/// (table, commit), mirroring how the changefeed groups per-table mutations, so
/// the write cost stays O(1) per modified table per transaction.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LiveEvents(pub Vec<LiveEvent>);

impl_kv_value_revisioned!(LiveEvents);

impl LiveEvents {
	pub(crate) fn new() -> Self {
		Self(Vec::new())
	}

	/// Append a record change, deriving the action from the before/after values.
	///
	/// Matches the changefeed's classification: a nullish `after` is a delete, an
	/// absent `before` is a create, otherwise it is an update.
	pub(crate) fn push_record_change(&mut self, id: RecordId, before: Value, after: Value) {
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
