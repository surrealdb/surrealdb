//! One export that runs a query on an in-memory datastore, so a wasm build
//! links every part of the engine a worker executing SurrealQL reaches.
//!
//! The export has three variants, chosen by features, each built for the size budget
//! (`scripts/wasm-size-budget.sh`):
//!
//! - lean (no features): `Datastore::execute` over the in-memory store alone.
//! - full (`full`): the same entry over every optional engine feature that builds for wasm32.
//! - edge (`edge-entry`): `Datastore::execute_edge` on a datastore built by `Builder::for_edge`,
//!   over the lean feature set. The entry reaches data statements and the edge builtins alone.
//!
//! A second export would be a linker root of its own and would grow the variant measured, so each
//! variant has the one.

// Computing the layout of the query future walks the engine's nested async
// calls, which is deeper than the default limit.
#![recursion_limit = "256"]

#[cfg(all(feature = "edge-entry", feature = "full"))]
compile_error!("edge-entry measures the lean feature set");

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use surrealdb_core::dbs::Session;
#[cfg(feature = "edge-entry")]
use surrealdb_core::kvs::Builder;
#[cfg(not(feature = "edge-entry"))]
use surrealdb_core::kvs::Datastore;

/// Runs the UTF-8 query at `ptr..ptr + len` and returns the number of
/// statement results, or 0 if it does not run.
///
/// # Safety
///
/// `ptr..ptr + len` must be readable and hold valid UTF-8.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn run(ptr: *const u8, len: usize) -> u32 {
	// SAFETY: the caller guarantees the range is readable UTF-8.
	let sql = unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len)) };
	#[cfg(not(feature = "edge-entry"))]
	let query = async move {
		let ds = Datastore::new("memory").await.ok()?;
		let results = ds.execute(sql, &Session::owner(), None).await.ok()?;
		u32::try_from(results.len()).ok()
	};
	#[cfg(feature = "edge-entry")]
	let query = async move {
		let ds = Builder::for_edge().build_with_path("memory").await.ok()?;
		let session = Session::owner().with_ns("test").with_db("test");
		let results = ds.execute_edge(sql, &session, None).await.ok()?;
		u32::try_from(results.len()).ok()
	};
	block_on(query).unwrap_or(0)
}

/// Polls `fut` to completion on the calling thread, spinning while it is
/// pending: the probe is built to be measured, not to serve queries.
fn block_on<F: Future>(fut: F) -> F::Output {
	let mut fut = pin!(fut);
	let mut cx = Context::from_waker(Waker::noop());
	loop {
		if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
			return v;
		}
	}
}
