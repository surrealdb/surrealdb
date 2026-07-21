//! `Send` assertion for HTTP client futures.
//!
//! On `wasm32-unknown-unknown`/`-none`, reqwest is backed by the JS `fetch`
//! API: its futures and response types hold JS values and are not `Send`,
//! while the query-execution futures that drive them are `Send`-bounded.
//! [`assert_send`] bridges that gap by wrapping the entire request/decode
//! block in a future that is manually declared `Send`. This is sound only
//! because those targets are single-threaded; If the build enables atomics
//! the future will no longer implement Send for
//!
//! On every other target — including `wasm32-wasip*`, where reqwest uses its
//! native, `Send` implementation — [`assert_send`] is the identity function
//! and merely asserts that the future already is `Send`.

use std::future::Future;

/// A future manually declared `Send` on single-threaded browser WASM.
#[repr(transparent)]
pub struct SendFuture<F>(F);

// SAFETY: This implements Send for non-send futures only on the web wasm target which is
// single-threaded.
#[cfg(all(
	target_arch = "wasm32",
	any(target_os = "unknown", target_os = "none"),
	not(target_feature = "atomics")
))]
unsafe impl<F> Send for SendFuture<F> {}

impl<F: Future> Future for SendFuture<F> {
	type Output = F::Output;

	fn poll(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Self::Output> {
		// SAFETY: This struct is just a wrapper around a future implementing send for it some
		// cases. Mapping pinning to it's inner future is thus sound.
		unsafe { self.map_unchecked_mut(|x| &mut x.0).poll(cx) }
	}
}

/// Wraps `fut` so it is `Send` in wasm; see the module docs for why this is sound.
pub fn assert_send<F: Future>(fut: F) -> SendFuture<F> {
	SendFuture(fut)
}
