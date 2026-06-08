//! Composer extension trait for supplying an [`ExecutionObserver`] at startup.

use std::sync::Arc;

use super::observer::{ExecutionObserver, NoopObserver};
use crate::CommunityComposer;

/// Composer extension trait for supplying an [`ExecutionObserver`] at startup.
///
/// The [`CommunityComposer`] returns [`NoopObserver`] by default; the server
/// crate wraps it with the real metrics observer separately. Enterprise
/// composers override this to return an audit observer which, in turn, wraps
/// the community metrics observer so both concerns fan out from a single
/// dispatch site.
pub trait ObservabilityProvider: requirements::ObservabilityProviderRequirements {
	/// Create the observer to install on the datastore at startup.
	fn create_observer(&self) -> Arc<dyn ExecutionObserver>;
}

/// Platform-specific auto-trait bounds required of [`ObservabilityProvider`]
/// implementations.
pub mod requirements {
	#[cfg(target_family = "wasm")]
	pub trait ObservabilityProviderRequirements {}

	#[cfg(not(target_family = "wasm"))]
	pub trait ObservabilityProviderRequirements: Send + Sync + 'static {}
}

impl requirements::ObservabilityProviderRequirements for CommunityComposer {}

impl ObservabilityProvider for CommunityComposer {
	fn create_observer(&self) -> Arc<dyn ExecutionObserver> {
		Arc::new(NoopObserver)
	}
}
