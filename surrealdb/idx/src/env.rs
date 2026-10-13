//! The environment the index engines run against.
//!
//! Every index engine — full-text, HNSW, DiskANN, count, and the doc-ID
//! allocator they share — needs the same handful of things from the query that
//! drives it: the transaction to read and write through, a cancellation
//! checkpoint for its scan loops, the process-local index stores, this node's
//! id, the table doc-ID sequence, and its own configuration.
//!
//! [`IndexEnv`] is exactly that set. It is declared here and implemented above,
//! so an engine names the environment it needs without naming the query context
//! that supplies it. Everything the trait hands back already lives at or below
//! this layer.
//!
//! Passed as `&dyn IndexEnv` rather than a generic parameter: the engines are
//! deep async call chains and monomorphising them over the environment would
//! duplicate every one of those state machines per implementation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use surrealdb_datastore::Transaction;
use surrealdb_datastore::sequences::Sequences;
use uuid::Uuid;

use crate::catalog::providers::CancellationProbe;
use crate::config::IdxConfig;
use crate::trees::store::IndexStores;

/// A boxed future returned by [`IndexEnv::is_done`].
///
/// Boxes at the trait boundary because the environment is held as a trait
/// object. `Send`, so a scan loop checking it stays usable from the streaming
/// executor's `Send` stream.
pub type BoxEnvFut<'a> = Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>;

/// What an index engine may ask of the query driving it.
///
/// `CancellationProbe` is a supertrait rather than an accessor because the
/// sequence allocator behind [`Self::sequences`] takes one directly: the
/// environment *is* the probe that interrupts a batch allocation, and it is
/// passed straight through. It also supplies the `Send + Sync` that lets an
/// engine hold the environment across an await.
pub trait IndexEnv: CancellationProbe {
	/// The transaction every read and write of this operation goes through.
	fn tx(&self) -> Arc<Transaction>;

	/// Returns `true` once the surrounding query has been cancelled or timed
	/// out, so the engine should stop; errors if the query has run past the
	/// memory threshold. Also yields to the runtime periodically, so a long
	/// scan cannot starve other tasks on the worker.
	///
	/// `count` is the caller's iteration number inside a scan loop and selects
	/// how much of the check to perform: cheap iterations read only the
	/// cancellation flag, while the deadline and memory checks run on a
	/// back-off schedule. Pass `None` outside a loop, for a full check every
	/// time.
	fn is_done(&self, count: Option<usize>) -> BoxEnvFut<'_>;

	/// The process-local registry of loaded indexes and their caches.
	fn index_stores(&self) -> &IndexStores;

	/// This node's id, written into the node-scoped keys that let concurrent
	/// writers append without contending.
	fn node_id(&self) -> Uuid;

	/// The distributed sequence manager backing the table doc-ID space.
	///
	/// Errors when the query runs in an environment that has none, which the
	/// doc-ID-consuming indexes treat as unreachable.
	fn sequences(&self) -> Result<&Sequences>;

	/// The index engines' own configuration.
	fn config(&self) -> &IdxConfig;
}

/// Forwards through an `Arc`, so a caller holding the environment behind one —
/// which every caller into the engines does — coerces it straight to
/// `&dyn IndexEnv` without unwrapping. Mirrors the same forward the supertrait
/// [`CancellationProbe`] declares.
impl<T: IndexEnv> IndexEnv for Arc<T> {
	fn tx(&self) -> Arc<Transaction> {
		T::tx(self)
	}

	fn is_done(&self, count: Option<usize>) -> BoxEnvFut<'_> {
		T::is_done(self, count)
	}

	fn index_stores(&self) -> &IndexStores {
		T::index_stores(self)
	}

	fn node_id(&self) -> Uuid {
		T::node_id(self)
	}

	fn sequences(&self) -> Result<&Sequences> {
		T::sequences(self)
	}

	fn config(&self) -> &IdxConfig {
		T::config(self)
	}
}
