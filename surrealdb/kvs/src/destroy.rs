//! Out-of-transaction range destruction: the optional backend capability that
//! drops a whole key range in one call instead of one write per key.

use std::any::Any;
use std::sync::Arc;

use common::future::BoxFut;

use crate::err::Result;
use crate::types::KeyRange;

/// A backend's ability to drop every version of every key in a half-open range
/// **outside** any transaction.
///
/// Contract on the caller: the range must already be logically unreachable —
/// typically because a committed transaction removed the catalog entry that
/// named it — because the destruction bypasses MVCC and is invisible to
/// conflict detection. A transaction whose snapshot predates the destruction
/// can see the keys vanish, and a transaction that read a key inside the range
/// can still commit, because nothing here enters any transaction's read or
/// write tracking.
///
/// Contract on the implementor: idempotent, so a repeat call on an
/// already-empty range succeeds; and **not** atomic across the range, so a
/// failed call may leave the range partly destroyed and must be safe to repeat.
/// `expunge` semantics do not apply — every version in the range goes,
/// whichever way the caller asked for the keys to be removed.
///
/// A backend offers this only while it can actually perform it. Resolving the
/// capability and getting nothing back is the signal to fall back to a
/// transactional delete; it is never reported as a successful destruction.
pub trait DestroyRange: Any + Send + Sync {
	/// Names the mechanism for diagnostics, e.g. the backend's own term for the
	/// primitive being used.
	fn mechanism(&self) -> &'static str;

	/// Drops every version of every key in `[range.start, range.end)`.
	fn destroy_range(&self, range: KeyRange<'static>) -> BoxFut<'_, Result<()>>;
}

/// The [`crate::TransactionBuilder::extension`] key under which a backend
/// publishes its [`DestroyRange`] implementation.
///
/// The newtype is what makes the capability recoverable: `Arc<dyn Any>`
/// downcasts only to a sized type, so a trait object cannot be recovered
/// directly and has to travel inside one.
pub struct DestroyRangeHandle(pub Arc<dyn DestroyRange>);
