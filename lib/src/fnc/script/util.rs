use crate::fnc::script::classes::headers::headers::Headers;
#[cfg(feature = "http")]
use surf::http;

#[macro_export]
macro_rules! throw_js_exception {
	($e:ident) => {
		js::Error::Exception {
			line: line!() as i32,
			message: $e.to_string(),
			file: file!().to_owned(),
			stack: "".to_owned(),
		}
	};
	($str:expr) => {
		js::Error::Exception {
			line: line!() as i32,
			message: $str.to_owned(),
			file: file!().to_owned(),
			stack: "".to_owned(),
		}
	};
}

// check js_object to see if value is a typed_array
pub fn is_typed_array<'js>(ctx: js::Ctx<'js>, value: js::Object<'js>) -> js::Result<bool> {
	let array_buffer: js::Object = ctx.globals().get("ArrayBuffer")?;
	let is_view: js::Function = array_buffer.get("isView")?;
	Ok(is_view.call((js::This(array_buffer), value))?)
}

#[cfg(feature = "http")]
// take http body from jsValue
pub fn take_http_body<'js>(
	ctx: js::Ctx<'js>,
	body: js::Value<'js>,
) -> js::Result<Option<surf::http::Body>> {
	use std::str::FromStr;

	use surf::http::Mime;

	use super::classes;

	if body.is_string() {
		// from string
		if let Some(js_str) = body.as_string() {
			return Ok(Some(http::Body::from_string(js_str.to_string()?)));
		}
	}
	if body.is_object() {
		if let Some(body) = body.into_object() {
			// from blob
			if body.instance_of::<classes::blob::blob::Blob>() {
				let v = body.into_instance::<classes::blob::blob::Blob>().unwrap();
				let v: &classes::blob::blob::Blob = v.as_ref();
				let mut body = surf::http::Body::from_bytes(v.data.clone());
				body.set_mime(Mime::from_str(v.mime.as_str()).map_err(|e| throw_js_exception!(e))?);
				return Ok(Some(body));
			}
			let array_buffer: js::Object = ctx.globals().get("ArrayBuffer")?;
			// from arrayBuffer
			if body.is_instance_of(array_buffer) {
				let js_ab = js::ArrayBuffer::from_object(body.clone())?;
				let buf: &[u8] = js_ab.as_ref();
				return Ok(Some(http::Body::from_bytes(buf.into())));
			}
			// from typedArray
			if is_typed_array(ctx.clone(), body.clone())? {
				// typedArray
				let js_ab: js::ArrayBuffer = body.get("buffer")?;
				let buf: &[u8] = js_ab.as_ref();
				return Ok(Some(http::Body::from_bytes(buf.into())));
			}
		}
	}
	Ok(None)
}

#[cfg(feature = "http")]
// take http headers from jsValue
pub fn take_http_headers<'js>(
	_ctx: js::Ctx<'js>,
	headers: js::Value<'js>,
) -> js::Result<Option<Headers>> {
	if headers.is_object() {
		// Extract the value as an object
		let headers = headers.into_object().unwrap();
		// Check to see if this object is a Headers
		if (headers).instance_of::<Headers>() {
			let headers = headers.into_instance::<Headers>().unwrap();
			let headers: &Headers = headers.as_ref();
			return Ok(Some(headers.clone()));
		} else {
			let mut map = Headers::new();
			for header in headers {
				if let Ok(header) = header {
					if let (Ok(name), Some(value)) =
						(header.0.to_string(), header.1.as_string().map(|rs| rs.to_string()))
					{
						if !name.is_empty() && value.is_ok() {
							map.set(name, value.unwrap())?;
						}
					}
				}
			}
			return Ok(Some(map));
		}
	}
	Ok(None)
}
