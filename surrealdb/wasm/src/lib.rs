//! The wasm-bindgen shim over an embedded SurrealDB engine.
//!
//! Everything here is FFI: turning a JavaScript options object into
//! [`surrealdb_embedded::Options`], moving bytes across the boundary, and
//! presenting live-query notifications as a `ReadableStream`. The database
//! behaviour lives in `surrealdb-embedded`, shared with the Node addon.
//!
//! This crate only exists on wasm targets; on native targets it compiles to
//! nothing, following `surrealdb-kvs-indxdb` — the IndexedDB backend it is
//! built against.
#![cfg(target_family = "wasm")]
// An RPC request's futures nest a `Value` tree deeply enough that laying out
// `execute`'s state machine exceeds the default query depth.
#![recursion_limit = "256"]

mod app;
mod err;

/// Reports panics to the browser console.
///
/// The default hook writes to a `stdout` a browser does not have, so without
/// this a panic reaches the page only as `unreachable executed` with no
/// message, file or line.
#[cfg(feature = "debug")]
mod debug {
	use std::panic;

	use wasm_bindgen::prelude::wasm_bindgen;

	#[wasm_bindgen(start)]
	fn init_panic_hook() {
		panic::set_hook(Box::new(|info| {
			// The error is constructed inside the panicking call so that its
			// `stack` records the wasm frames that led here; this target has no
			// Rust backtrace to fall back on. Logging `stack` rather than the
			// error carries the message as well and reaches hosts whose console
			// renders only an error's message.
			let error = js_sys::Error::new(&info.to_string());
			let stack = js_sys::Reflect::get(&error, &wasm_bindgen::JsValue::from_str("stack"))
				.unwrap_or_else(|_| error.into());
			web_sys::console::error_1(&stack);
		}));
	}
}
