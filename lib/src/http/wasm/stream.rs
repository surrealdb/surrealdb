use futures::Stream;
use std::{
	pin::Pin,
	task::{Context, Poll},
};
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{ReadableStream, ReadableStreamDefaultReader};

enum State {
	Initial,
	Waiting,
	Done,
}

pub struct WasmStream {
	reader: ReadableStreamDefaultReader,
	state: State,
}

impl WasmStream {
	pub fn new(stream: ReadableStream) -> Self {
		let reader = stream.get_reader().unchecked_into();
		Self {
			reader,
			state: State::Initial,
		}
	}
}

impl Drop for WasmStream {
	fn drop(&mut self) {
		self.reader.cancel();
	}
}

impl Stream for WasmStream {
	type Item = Result<JsValue, JsValue>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		todo!()
	}
}
