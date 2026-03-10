//! This module stores the TiKV savepoint type.

use crate::kvs::{Key, Val};

/// A savepoint state capturing operations that can be undone
#[derive(Debug, Clone)]
pub(super) struct Savepoint {
	/// Operations that can be undone to rollback to this savepoint
	pub(crate) operations: Vec<Operation>,
}

/// An operation that can be undone during savepoint rollback
#[derive(Debug, Clone)]
pub(super) enum Operation {
	/// Delete a key that was inserted
	DeleteKey(Key),
	/// Restore a key to its previous value
	RestoreValue(Key, Val),
	/// Restore a key that was deleted (insert it back)
	RestoreDeleted(Key, Val),
}
