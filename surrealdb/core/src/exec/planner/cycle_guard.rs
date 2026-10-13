//! Plan-time cycle detection for permission and computed-field compilation.
//!
//! Permissions like `WHERE (SELECT FROM same_table) != NONE` re-enter
//! table-context resolution while the outer pass is still in progress.
//! `try_resolve_table_ctx` pushes a [`CycleGuardEntry`] before recursing
//! and bails when the key is already present, falling back to txn-less
//! compilation for that subtree.
//!
//! The guard also counts the lowerings of each table's events for each action
//! open on the planners that share it ([`CycleGuard::try_enter_events`]),
//! which bounds a chain of events that write back to their own table in
//! frames that never repeat.
//!
//! It also holds the units whose planning is open on the planners that share
//! it ([`CycleGuard::try_enter_unit`]), keyed by what the unit is (its
//! request: a function body or guard, a table's read unit, events, a view's
//! recalculation, a cascade) and the frame it is planned for. A compile
//! interns each unit it asks for once and plans its requests one after
//! another, so a unit that recurses into itself within one compile links to
//! the slot it already holds and the recursion runs, spending the
//! computation-depth budget. A unit asked for again from a compile its own
//! planning started has no slot of that compile to link to, and is refused
//! where it is entered again.
//!
//! Orthogonal to the runtime data-cycle guards (`skip_fetch_perms`,
//! `computing_record`) — those break cycles during evaluation, this one
//! breaks them during plan construction.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use surrealdb_strand::TableName;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::doc::Action;
use crate::exec::vm::ir_lower::UnitRequest;
use crate::exec::vm::program::UnitFrame;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TableKey {
	ns: NamespaceId,
	db: DatabaseId,
	table: TableName,
}

#[derive(Clone, Default)]
pub(crate) struct CycleGuard {
	inner: Arc<Mutex<HashSet<TableKey>>>,
	/// How many lowerings of each table's events for each action are open.
	events: Arc<Mutex<HashMap<EventsKey, usize>>>,
	/// The units whose planning is open.
	units: Arc<Mutex<HashSet<UnitKey>>>,
}

/// A unit a compile plans: what it is, and the frame it is planned for.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct UnitKey {
	request: UnitRequest,
	frame: UnitFrame,
}

impl UnitKey {
	/// The unit `request` names, planned for `frame`.
	pub(crate) fn new(request: UnitRequest, frame: UnitFrame) -> Self {
		Self {
			request,
			frame,
		}
	}
}

/// A table's events for one action.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EventsKey {
	table: TableKey,
	action: Action,
}

impl CycleGuard {
	/// Whether no compilation of a table and no lowering of a table's events
	/// is open on the guard: what a planner holding it compiles is then
	/// what a fresh planner compiles. An open unit changes nothing a planner
	/// compiles, so it does not count.
	pub(crate) fn is_idle(&self) -> bool {
		self.inner.lock().is_empty() && self.events.lock().is_empty()
	}

	/// Try to push `(ns, db, table)` onto the guard. Returns `Some(entry)`
	/// on success — when `entry` drops, the key is removed. Returns `None`
	/// if the key is already present; the caller must fall back to a
	/// txn-less compilation path for the affected subtree.
	#[must_use]
	pub(crate) fn try_enter(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		table: TableName,
	) -> Option<CycleGuardEntry> {
		let key = TableKey {
			ns,
			db,
			table,
		};
		let mut guard = self.inner.lock();
		if guard.contains(&key) {
			None
		} else {
			guard.insert(key.clone());
			Some(CycleGuardEntry {
				guard: Arc::clone(&self.inner),
				key,
			})
		}
	}

	/// Try to push the unit `key` onto the units whose planning is open.
	/// Returns `Some(entry)` on success — when `entry` drops, the unit is
	/// closed. Returns `None` if its planning is open already: planning it now
	/// is a cycle through a compile its own planning started.
	#[must_use]
	pub(crate) fn try_enter_unit(&self, key: UnitKey) -> Option<UnitEntry> {
		let mut units = self.units.lock();
		if !units.insert(key.clone()) {
			return None;
		}
		Some(UnitEntry {
			units: Arc::clone(&self.units),
			key,
		})
	}

	#[cfg(test)]
	pub(crate) fn contains(&self, ns: NamespaceId, db: DatabaseId, table: &TableName) -> bool {
		self.inner.lock().contains(&TableKey {
			ns,
			db,
			table: table.clone(),
		})
	}
}

pub(crate) struct CycleGuardEntry {
	guard: Arc<Mutex<HashSet<TableKey>>>,
	key: TableKey,
}

impl Drop for CycleGuardEntry {
	fn drop(&mut self) {
		self.guard.lock().remove(&self.key);
	}
}

/// A unit whose planning is open, closed when dropped.
pub(crate) struct UnitEntry {
	units: Arc<Mutex<HashSet<UnitKey>>>,
	key: UnitKey,
}

impl Drop for UnitEntry {
	fn drop(&mut self) {
		self.units.lock().remove(&self.key);
	}
}
