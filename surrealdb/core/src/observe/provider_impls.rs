//! Core-side implementations of the observability provider traits for
//! [`CommunityComposer`].
//!
//! The trait surface lives in the leaf `surrealdb-observe` crate; these `impl`s
//! stay in core because they name [`CommunityComposer`], a core type (orphan
//! rule: local type + foreign trait).

use std::sync::Arc;

use surrealdb_observe::observer::{ExecutionObserver, NoopObserver};
use surrealdb_observe::provider::ObservabilityProvider;

use crate::CommunityComposer;

impl ObservabilityProvider for CommunityComposer {
	fn create_observer(&self) -> Arc<dyn ExecutionObserver> {
		Arc::new(NoopObserver)
	}
}
