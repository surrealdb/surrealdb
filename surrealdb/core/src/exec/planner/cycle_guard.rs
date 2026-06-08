//! Plan-time cycle detection for permission and computed-field compilation.
//!
//! Permissions like `WHERE (SELECT FROM same_table) != NONE` re-enter
//! table-context resolution while the outer pass is still in progress.
//! `try_resolve_table_ctx` pushes a [`CycleGuardEntry`] before recursing
//! and bails when the key is already present, falling back to txn-less
//! compilation for that subtree.
//!
//! Orthogonal to the runtime data-cycle guards (`skip_fetch_perms`,
//! `computing_record`) — those break cycles during evaluation, this one
//! breaks them during plan construction.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::val::TableName;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TableKey {
	ns: NamespaceId,
	db: DatabaseId,
	table: TableName,
}

#[derive(Clone, Default)]
pub(crate) struct CycleGuard {
	inner: Arc<Mutex<HashSet<TableKey>>>,
}

impl CycleGuard {
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

	#[cfg(test)]
	fn contains(&self, ns: NamespaceId, db: DatabaseId, table: &TableName) -> bool {
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::TableName;

	fn key(name: &str) -> (NamespaceId, DatabaseId, TableName) {
		(NamespaceId(1), DatabaseId(2), TableName::new(name))
	}

	#[test]
	fn empty_guard_does_not_contain_anything() {
		let g = CycleGuard::default();
		let (ns, db, t) = key("foo");
		assert!(!g.contains(ns, db, &t));
	}

	#[test]
	fn try_enter_succeeds_on_empty_guard() {
		let g = CycleGuard::default();
		let (ns, db, t) = key("foo");
		let entry = g.try_enter(ns, db, t.clone());
		assert!(entry.is_some());
		assert!(g.contains(ns, db, &t));
	}

	#[test]
	fn try_enter_fails_when_key_present() {
		let g = CycleGuard::default();
		let (ns, db, t) = key("foo");
		let _outer = g.try_enter(ns, db, t.clone()).expect("first push must succeed");
		let inner = g.try_enter(ns, db, t);
		assert!(inner.is_none(), "second push of same key must return None");
	}

	#[test]
	fn drop_removes_entry() {
		let g = CycleGuard::default();
		let (ns, db, t) = key("foo");
		{
			let _entry = g.try_enter(ns, db, t.clone()).expect("push");
			assert!(g.contains(ns, db, &t));
		}
		assert!(!g.contains(ns, db, &t), "entry should be removed when guard drops");
	}

	#[test]
	fn distinct_keys_coexist() {
		let g = CycleGuard::default();
		let (ns, db, t1) = key("foo");
		let (_, _, t2) = key("bar");
		let _e1 = g.try_enter(ns, db, t1.clone()).expect("push t1");
		let _e2 = g.try_enter(ns, db, t2.clone()).expect("push t2");
		assert!(g.contains(ns, db, &t1));
		assert!(g.contains(ns, db, &t2));
	}

	#[test]
	fn re_entry_after_drop_succeeds() {
		let g = CycleGuard::default();
		let (ns, db, t) = key("foo");
		drop(g.try_enter(ns, db, t.clone()).expect("first push"));
		let again = g.try_enter(ns, db, t);
		assert!(again.is_some(), "re-entry after drop should succeed");
	}

	#[test]
	fn cloned_guards_share_state() {
		let g1 = CycleGuard::default();
		let g2 = g1.clone();
		let (ns, db, t) = key("foo");
		let _entry = g1.try_enter(ns, db, t.clone()).expect("push via g1");
		assert!(g2.contains(ns, db, &t), "g2 should see the key pushed via g1");
		assert!(g2.try_enter(ns, db, t).is_none(), "g2's try_enter for the same key must fail");
	}

	#[test]
	fn cross_database_keys_are_distinct() {
		let g = CycleGuard::default();
		let t = TableName::new("foo");
		let _e1 = g.try_enter(NamespaceId(1), DatabaseId(2), t.clone()).expect("(ns=1, db=2)");
		// Same table name in a different database is not a cycle.
		let e2 = g.try_enter(NamespaceId(1), DatabaseId(3), t);
		assert!(e2.is_some(), "same table name in different db must not collide");
	}
}
