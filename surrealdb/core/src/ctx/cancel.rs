use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio_util::sync::CancellationToken;

/// External cancellation handle plumbed from connection-owning layers
/// (e.g. the WebSocket RPC server) into the executor's [`super::Context`].
///
/// Cancellation has two views:
///
/// * `flag` — a hot-path `AtomicBool` checked by [`super::Context::done`] at every executor yield
///   (statement boundaries, iterator hot loops, HNSW/DiskANN search). Reading this is
///   single-digit-nanoseconds.
/// * `token` — a `tokio_util::sync::CancellationToken` suitable for `select!`-based races. Lets
///   points that bare-await on an external timer (e.g. `SLEEP`) be interrupted instead of running
///   to completion.
///
/// Both views are tripped together by [`Self::trip`] so executor-driven
/// code and bare-await code converge on the same cancel decision. Callers
/// who only need *one* view can still use it independently — e.g. the
/// streaming-exec layer holds the token on its own
/// [`crate::exec::context::ExecutionContext`] for `select!` use.
#[derive(Clone, Debug)]
pub struct CancelHandle {
	flag: Arc<AtomicBool>,
	token: CancellationToken,
}

impl Default for CancelHandle {
	fn default() -> Self {
		Self::new()
	}
}

impl CancelHandle {
	/// Construct a fresh, untripped handle.
	pub fn new() -> Self {
		Self {
			flag: Arc::new(AtomicBool::new(false)),
			token: CancellationToken::new(),
		}
	}

	/// The `AtomicBool` view, suitable for installing on a
	/// [`super::Context`] via [`super::Context::set_cancellation`].
	pub fn flag(&self) -> Arc<AtomicBool> {
		Arc::clone(&self.flag)
	}

	/// The `CancellationToken` view, suitable for `select!`-based races
	/// against external timers. Child tokens (via
	/// [`CancellationToken::child_token`]) inherit cancellation, so the
	/// executor's per-query token can be derived from this without losing
	/// the connection-level cancel signal.
	pub fn token(&self) -> CancellationToken {
		self.token.clone()
	}

	/// Returns `true` once [`Self::trip`] has been called.
	///
	/// Both views are checked: the flag covers the executor's hot path,
	/// the token covers the (rarer) case where only the token was tripped
	/// by something other than `trip` itself.
	pub fn is_cancelled(&self) -> bool {
		self.flag.load(Ordering::Relaxed) || self.token.is_cancelled()
	}

	/// Trip both views in lockstep. Idempotent.
	pub fn trip(&self) {
		self.flag.store(true, Ordering::Relaxed);
		self.token.cancel();
	}

	/// Await cancellation. Convenience over `self.token().cancelled()`.
	pub async fn cancelled(&self) {
		self.token.cancelled().await
	}
}
