//! The IndexedDB (browser/WASM) key-value store backend for SurrealDB.
//!
//! Implemented directly on top of the browser's IndexedDB API through a small
//! JavaScript driver (`src/js/db.js`). IndexedDB transactions go inactive
//! whenever an await crosses a non-IndexedDB microtask, so a native IndexedDB
//! transaction cannot back a kvs transaction. Instead each transaction tracks
//! its read-set and write-set in memory ([`tx::IndxbTx`]) and commits through
//! a single JavaScript call that validates the read-set and applies the
//! write-set inside one IndexedDB transaction — optimistic concurrency
//! control, with conflicts reported as [`Error::TransactionConflict`].
//!
//! This backend only exists on WASM targets; on native targets this crate
//! compiles to nothing.
#![cfg(target_family = "wasm")]

// The unsafe `Send`/`Sync` impls in `ffi` and `send_future` are sound only
// because wasm without threads is single-threaded. Turn that precondition
// into a compile-time invariant instead of a comment.
#[cfg(target_feature = "atomics")]
compile_error!(
	"surrealdb-kvs-indxdb relies on single-threaded WASM for its `Send`/`Sync` \
	 soundness and cannot be built with the `atomics` target feature"
);

mod ffi;
mod tx;

use surrealdb_kvs::api::{BoxFut, Transactable};
use surrealdb_kvs::{Error, Metrics, Result, TransactionBuilder, TransactionType};
use tracing::instrument;
use wasm_bindgen::JsValue;

use crate::tx::IndxdbTx;

/// Maps a JavaScript exception to a kvs error.
fn kvs_error(v: JsValue) -> Error {
	if let Some(msg) = ffi::to_string(v) {
		Error::internal(msg)
	} else {
		Error::internal("Unknown error")
	}
}

pub struct Datastore {
	db: ffi::Db,
}

impl Datastore {
	/// Open a new database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::indxdb", skip(path))]
	pub async fn new(path: &str) -> Result<Datastore> {
		let db = ffi::Db::open(path).await.map_err(kvs_error)?;
		Ok(Datastore {
			db,
		})
	}

	/// Shutdown the database
	#[instrument(level = "trace", target = "surrealdb::core::kvs::indxdb", skip(self))]
	pub async fn shutdown(&self) -> Result<()> {
		self.db.close().map_err(kvs_error)
	}

	/// Start a new transaction
	#[instrument(level = "trace", target = "surrealdb::core::kvs::indxdb", skip_all)]
	pub async fn transaction(&self, write: TransactionType) -> Result<Box<dyn Transactable>> {
		let tx = self.db.begin().map_err(kvs_error)?;
		Ok(Box::new(IndxdbTx::new(write, tx)))
	}
}

impl TransactionBuilder for Datastore {
	fn name(&self) -> &'static str {
		"indxdb"
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>> {
		// Transactions are local: the store runs in-process.
		Box::pin(async move { Ok((self.transaction(write).await?, true)) })
	}

	fn shutdown(&self) -> BoxFut<'_, Result<()>> {
		Box::pin(Datastore::shutdown(self))
	}

	fn register_metrics(&self) -> Option<Metrics> {
		None
	}

	fn collect_u64_metric(&self, _metric: &str) -> Option<u64> {
		None
	}
}

const _: () = {
	const fn is_send_sync<T: Send + Sync>() {}
	is_send_sync::<Datastore>();
};
