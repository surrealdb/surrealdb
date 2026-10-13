//! Composer extension trait for supplying an [`ExecutionObserver`] at startup.

use std::sync::Arc;

use crate::observer::ExecutionObserver;

/// Composer extension trait for supplying an [`ExecutionObserver`] at startup.
///
/// The community composer returns a no-op observer by default; the server
/// crate wraps it with the real metrics observer separately. Enterprise
/// composers override this to return an audit observer which, in turn, wraps
/// the community metrics observer so both concerns fan out from a single
/// dispatch site.
///
/// The concrete `impl` for the community composer lives in `surrealdb-core`
/// (next to the composer type); this crate only defines the trait surface.
pub trait ObservabilityProvider: Send + Sync + 'static {
	/// Create the observer to install on the datastore at startup.
	fn create_observer(&self) -> Arc<dyn ExecutionObserver>;
}
