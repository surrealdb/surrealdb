use crate::fnc::script::classes::headers::headers::Headers;
use crate::fnc::script::classes::response::response::Response;
use crate::throw_js_exception;
use js::Rest;
use js::Result;
use std::str::FromStr;
use surf::http;

pub(crate) struct FetchOptions {
	method: http::Method,
	headers: Headers,
	body: Option<http::Body>,
}

impl Default for FetchOptions {
	fn default() -> Self {
		Self {
			method: http::Method::Get,
			headers: Headers::new(),
			body: None,
		}
	}
}

// check js_object to see if value is a typed_array
fn is_typed_array<'js>(ctx: js::Ctx<'js>, value: js::Object<'js>) -> js::Result<bool> {
	let array_buffer: js::Object = ctx.globals().get("ArrayBuffer")?;
	let is_view: js::Function = array_buffer.get("isView")?;
	Ok(is_view.call((js::This(array_buffer), value))?)
}

impl<'js> js::FromJs<'js> for FetchOptions {
	fn from_js(ctx: js::Ctx<'js>, value: js::Value<'js>) -> js::Result<Self> {
		match value {
			val if val.is_object() => {
				let mut options = FetchOptions::default();
				let val = val.into_object().unwrap();
				if let Ok(method) = val.get::<_, String>("method") {
					options.method =
						http::Method::from_str(&method).map_err(|e| throw_js_exception!(e))?;
				}
				if let Ok(headers) = val.get::<_, js::Value<'js>>("headers") {
					if headers.is_object() {
						// Extract the value as an object
						let headers = headers.into_object().unwrap();
						// Check to see if this object is a Headers
						if (headers).instance_of::<Headers>() {
							let headers = headers.into_instance::<Headers>().unwrap();
							let headers: &Headers = headers.as_ref();
							options.headers = headers.clone();
						} else {
							let mut map = Headers::new();
							for header in headers {
								if let Ok(header) = header {
									if let (Ok(name), Some(value)) = (
										header.0.to_string(),
										header.1.as_string().map(|rs| rs.to_string()),
									) {
										if !name.is_empty() && value.is_ok() {
											map.set(name, value.unwrap())?;
										}
									}
								}
							}
							options.headers = map;
						}
					}
				}
				if let Ok(body) = val.get::<_, js::Value<'js>>("body") {
					if body.is_string() {
						if let Some(js_str) = body.as_string() {
							options.body = Some(http::Body::from_string(js_str.to_string()?))
						}
					}
					if body.is_object() {
						if let Some(body) = body.into_object() {
							let array_buffer: js::Object = ctx.globals().get("ArrayBuffer")?;
							if body.is_instance_of(array_buffer) {
								// arraybuffer
								let js_ab = js::ArrayBuffer::from_object(body.clone())?;
								let buf: &[u8] = js_ab.as_ref();
								options.body = Some(http::Body::from_bytes(buf.into()))
							} else if is_typed_array(ctx.clone(), body.clone())? {
								// typedArray
								let js_ab: js::ArrayBuffer = body.get("buffer")?;
								let buf: &[u8] = js_ab.as_ref();
								options.body = Some(http::Body::from_bytes(buf.into()))
							}
						}
					}
				}
				Ok(options)
			}
			_ => Ok(Default::default()),
		}
	}
}

#[js::bind(object, public)]
#[quickjs(rename = "fetch")]
#[allow(unused_variables)]
pub(crate) async fn fetch(url: String, mut optional: Rest<FetchOptions>) -> Result<Response> {
	let mut options: FetchOptions = Default::default();
	if let Some(_options) = optional.pop() {
		options = _options;
	}
	let url = http::Url::parse(&url).map_err(|e| throw_js_exception!(e))?;

	let client = surf::client().with(surf::middleware::Redirect::new(5));

	let rb = surf::RequestBuilder::new(options.method, url);
	let mut req = rb.build();
	for (key, values) in options.headers {
		for value in values {
			req.append_header(key.clone(), value)
		}
	}
	if let Some(body) = options.body {
		req.set_body(body);
	}
	let resp = client.send(req).await.map_err(|e| throw_js_exception!(e))?;
	Ok(Response::from_surf(resp))
}
