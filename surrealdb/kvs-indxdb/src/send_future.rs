use std::pin::Pin;
use std::task::{Context, Poll};

use js_sys::Promise;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;

/// A `Send` future resolving a JavaScript [`Promise`].
///
/// The kvs `Transactable` trait requires `Send` futures. JS values are not
/// `Send`, but WASM without threads is single-threaded, making it sound to
/// declare this future `Send` manually (`lib.rs` rejects `atomics` builds at
/// compile time). All of the actual promise plumbing is delegated to
/// [`JsFuture`].
pub struct SendFuture(JsFuture);

// SAFETY: wasm32-unknown-unknown without the `atomics` target feature is
// single-threaded: every value of this type is created, polled and dropped on
// the one and only thread, so no data race can occur even though the wrapped
// `JsFuture` is not thread-safe.
unsafe impl Send for SendFuture {}

impl SendFuture {
	pub fn new(promise: Promise) -> Self {
		SendFuture(JsFuture::from(promise))
	}
}

impl Future for SendFuture {
	type Output = Result<JsValue, JsValue>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		// `JsFuture` is `Unpin`, so re-pinning the field is trivially sound.
		Pin::new(&mut self.0).poll(cx)
	}
}

const _: () = {
	const fn is_send<T: Send>() {}
	is_send::<SendFuture>();
};
