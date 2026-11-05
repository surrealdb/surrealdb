//! Backend maintenance: the optional capability a backend offers when it keeps
//! MVCC versions and transactional locks that the engine must periodically
//! reclaim on its behalf.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use common::future::BoxFut;

use crate::err::Result;

/// A backend's periodic maintenance operations.
///
/// The engine schedules these on its background cadence; the backend decides
/// what each pass means for its own storage. Every method is safe to repeat and
/// to run while transactions are in flight.
pub trait Maintenance: Any + Send + Sync {
	/// Advances the garbage-collection safepoint to `now - lifetime`, letting
	/// the backend discard MVCC versions no snapshot younger than `lifetime`
	/// can read.
	///
	/// A backend with garbage collection switched off in its own configuration
	/// completes without doing anything.
	fn collect_garbage(&self, lifetime: Duration) -> BoxFut<'_, Result<()>>;

	/// Resolves transactional locks abandoned by crashed clients and older than
	/// `lifetime`, which would otherwise block readers of the locked keys.
	fn cleanup_locks(&self, lifetime: Duration) -> BoxFut<'_, Result<()>>;

	/// The number of transactions currently open on this datastore, or `None`
	/// when the backend does not track them.
	fn in_flight_transactions(&self) -> Option<usize>;
}

/// The [`crate::TransactionBuilder::extension`] key under which a backend
/// publishes its [`Maintenance`] implementation.
///
/// The newtype is what makes the capability recoverable: `Arc<dyn Any>`
/// downcasts only to a sized type, so a trait object cannot be recovered
/// directly and has to travel inside one.
pub struct MaintenanceHandle(pub Arc<dyn Maintenance>);
