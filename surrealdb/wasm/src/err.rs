//! The error every exported method reports.
//!
//! wasm-bindgen turns an `Err` into a thrown JavaScript value, so this is only
//! ever a message: the engine's errors already carry their own text, and the
//! JavaScript SDK reads a *method* failure off the RPC reply envelope rather
//! than from a throw.

use wasm_bindgen::JsValue;

#[derive(Debug)]
pub struct Error(JsValue);

impl From<Error> for JsValue {
	fn from(Error(value): Error) -> Self {
		value
	}
}

impl From<anyhow::Error> for Error {
	fn from(error: anyhow::Error) -> Self {
		// `{:#}` so the context an engine error was built with survives, rather
		// than reaching JavaScript as just the outermost message.
		Self(JsValue::from(format!("{error:#}")))
	}
}

impl From<serde_wasm_bindgen::Error> for Error {
	fn from(error: serde_wasm_bindgen::Error) -> Self {
		Self(JsValue::from(error.to_string()))
	}
}
