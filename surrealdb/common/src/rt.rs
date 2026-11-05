//! Task spawning, over whichever executor the target has.
//!
//! Every call site that starts a background task takes the spawner from here,
//! so the choice of executor is made once per target rather than per file.
//!
//! - **Native.** Tokio. The caller runs inside a tokio runtime, multi- or current-thread.
//! - **Non-browser wasm targets** (WASI). Tokio's current-thread runtime, which the host drives
//!   from its own event loop. There are no worker threads, so a spawned task makes progress only
//!   while the host polls that runtime, and the caller must be inside it (a runtime context, as for
//!   [`tokio::spawn`]). [`spawn`] still returns a [`JoinHandle`], so code that joins or aborts its
//!   tasks is the same code as on native.
//! - **Browser wasm** (`wasm32-unknown-unknown`). `wasm_bindgen_futures` on the JavaScript
//!   microtask queue. That executor has no join handle, so only [`spawn_detached`] exists there,
//!   and it accepts a future that is not `Send`.
//!
//! The browser is told apart by `target_os = "unknown"`, the only wasm OS
//! whose executor and clock come from JavaScript.

use std::future::Future;

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
pub use tokio::task::{JoinError, JoinHandle};

/// Spawns `fut` on the ambient tokio runtime and returns its handle.
///
/// Dropping the handle detaches the task; [`JoinHandle::abort`] cancels it at
/// its next await point. Panics outside a tokio runtime context.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
pub fn spawn<F>(fut: F) -> JoinHandle<F::Output>
where
	F: Future + Send + 'static,
	F::Output: Send + 'static,
{
	tokio::spawn(fut)
}

/// Spawns `fut` and lets it run to completion with nothing to join.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
pub fn spawn_detached<F>(fut: F)
where
	F: Future<Output = ()> + Send + 'static,
{
	drop(tokio::spawn(fut));
}

/// Spawns `fut` on the JavaScript microtask queue and lets it run to
/// completion with nothing to join.
#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub fn spawn_detached<F>(fut: F)
where
	F: Future<Output = ()> + 'static,
{
	wasm_bindgen_futures::spawn_local(fut);
}
