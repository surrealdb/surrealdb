use futures::Stream;
use js_sys::Promise;
use std::{
	cell::Cell,
	pin::Pin,
	rc::Rc,
	task::{Context, Poll, Waker},
};
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use web_sys::{ReadableStream, ReadableStreamDefaultReader};

pub struct Transfer {
	result: Cell<Option<Result<JsValue, JsValue>>>,
	done: Cell<bool>,
	waker: Cell<Option<Waker>>,
}

impl Transfer {
	fn resolve(&self, value: JsValue) {
		match js_sys::Reflect::get(&value, &JsValue::from_str("done")) {
			Ok(x) => {
				if x.is_truthy() {
					self.done.set(true);
				} else {
					match js_sys::Reflect::get(&value, &JsValue::from_str("value")) {
						Ok(x) => {
							self.result.set(Some(Ok(x)));
						}
						Err(e) => {
							self.result.set(Some(Err(e)));
						}
					}
				}
			}
			Err(e) => {
				self.result.set(Some(Ok(e)));
			}
		}
		self.waker.take().unwrap().wake();
	}

	fn reject(&self, value: JsValue) {
		self.result.set(Some(Err(value)));
		self.waker.take().unwrap().wake();
	}
}

enum State {
	Initial,
	Waiting {
		promise: Promise,
		transfer: Rc<Transfer>,
	},
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
		let _ = self.reader.cancel();
	}
}

impl Stream for WasmStream {
	type Item = Result<JsValue, JsValue>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		let this = self.get_mut();

		match this.state {
			State::Done => Poll::Ready(None),
			State::Initial => {
				let transfer = Rc::new(Transfer {
					result: Cell::new(None),
					done: Cell::new(false),
					waker: Cell::new(Some(cx.waker().clone())),
				});
				let trans = transfer.clone();
				let resolve_closure = Closure::new(move |value| trans.resolve(value));
				let trans = transfer.clone();
				let reject_closure = Closure::new(move |value| trans.reject(value));

				let promise = this.reader.read().then2(&resolve_closure, &reject_closure);

				this.state = State::Waiting {
					promise,
					transfer,
				};
				Poll::Pending
			}
			State::Waiting {
				ref transfer,
				ref mut promise,
			} => {
				if transfer.done.get() {
					this.state = State::Done;
					return Poll::Ready(None);
				};

				transfer.waker.set(Some(cx.waker().clone()));
				let Some(result) = transfer.result.take() else {
					return Poll::Pending;
				};
				let trans = transfer.clone();
				let resolve_closure = Closure::new(move |value| trans.resolve(value));
				let trans = transfer.clone();
				let reject_closure = Closure::new(move |value| trans.reject(value));
				*promise = this.reader.read().then2(&resolve_closure, &reject_closure);
				Poll::Ready(Some(result))
			}
		}
	}
}
