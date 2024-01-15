use bytes::Bytes;
use wasm_bindgen::JsValue;

enum Kind {
	Bytes(Bytes),
}

pub struct Body {
	kind: Kind,
}

impl Body {
	pub fn empty() -> Self {
		Self {
			kind: Kind::Bytes(Bytes::new()),
		}
	}

	pub fn into_js_value(self) -> JsValue {
		match self.kind {
			Kind::Bytes(x) => {
				// u32 should always be big enough since webassembly has 32bit pointers
				let v = js_sys::Uint8Array::new_with_length(x.len() as u32);
				v.copy_from(&x);
				v.into()
			}
		}
	}
}

impl<B> From<B> for Body
where
	Bytes: From<B>,
{
	fn from(b: B) -> Self {
		Body {
			kind: Kind::Bytes(Bytes::from(b)),
		}
	}
}
