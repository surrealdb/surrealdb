//! Headers class implementation

use std::borrow::Cow;
use std::str::FromStr;

use js::class::Trace;
use js::prelude::{Coerced, List};
use js::{Array, Ctx, Exception, JsLifetime, Result, Value};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

/// Renders a header value as the string the script sees.
///
/// A `Headers` built from a response carries the values a remote server sent.
/// HTTP header values are opaque bytes (RFC 9110) and `HeaderValue::from_bytes`
/// accepts any byte outside a narrow set of control characters, whereas
/// `HeaderValue::to_str` succeeds only for values made entirely of visible
/// ASCII. Any non-ASCII byte therefore reaches this function as an error, so a
/// fallback is required rather than optional: a panic in this position aborts
/// the process under the release profile's `panic = "abort"`.
///
/// Visible-ASCII values are borrowed unchanged, so every value a script could
/// already read keeps its exact representation. Anything else is decoded from
/// UTF-8 lossily, which renders valid multi-byte UTF-8 correctly and maps only
/// genuinely malformed sequences to U+FFFD.
fn header_value_to_string(value: &HeaderValue) -> Cow<'_, str> {
	match value.to_str() {
		Ok(v) => Cow::Borrowed(v),
		Err(_) => String::from_utf8_lossy(value.as_bytes()),
	}
}

#[derive(Clone, Trace, JsLifetime)]
#[js::class]
pub struct Headers {
	#[qjs(skip_trace)]
	pub(crate) inner: HeaderMap,
}

#[js::methods]
impl Headers {
	// ------------------------------
	// Constructor
	// ------------------------------

	#[qjs(constructor)]
	pub fn new<'js>(ctx: Ctx<'js>, init: Value<'js>) -> Result<Self> {
		Headers::new_inner(&ctx, init)
	}

	// ------------------------------
	// Instance methods
	// ------------------------------

	// Convert the object to a string
	#[qjs(rename = "toString")]
	pub fn js_to_string(&self) -> String {
		String::from("[object Header]")
	}

	// Adds or appends a new value to a header
	pub fn append(&mut self, ctx: Ctx<'_>, key: String, val: String) -> Result<()> {
		self.append_inner(&ctx, &key, &val)
	}

	// Deletes a header from the header set
	pub fn delete(&mut self, ctx: Ctx<'_>, key: String) -> Result<()> {
		// Process and check the header name is valid
		let key =
			HeaderName::from_str(&key).map_err(|e| Exception::throw_type(&ctx, &format!("{e}")))?;
		// Remove the header entry from the map
		self.inner.remove(&key);
		// Everything ok
		Ok(())
	}

	// Returns all header entries in the header set
	pub fn entries(&self) -> Vec<List<(String, String)>> {
		let mut res = Vec::<List<(String, String)>>::with_capacity(self.inner.len());

		for (k, v) in self.inner.iter() {
			let k = k.as_str();
			if Some(k) == res.last().map(|x| x.0.0.as_str()) {
				let ent = res.last_mut().expect("last element exists");
				ent.0.1.push_str(", ");
				ent.0.1.push_str(&header_value_to_string(v));
			} else {
				res.push(List((k.to_owned(), header_value_to_string(v).into_owned())));
			}
		}

		res
	}

	// Returns all values of a header in the header set
	pub fn get(&self, ctx: Ctx<'_>, key: String) -> Result<Option<String>> {
		// Process and check the header name is valid
		let key =
			HeaderName::from_str(&key).map_err(|e| Exception::throw_type(&ctx, &format!("{e}")))?;
		// Convert the header values to strings
		let all = self.inner.get_all(&key);

		let mut res = String::new();
		for (idx, v) in all.iter().enumerate() {
			if idx != 0 {
				res.push_str(", ");
			}
			res.push_str(&header_value_to_string(v));
		}

		if res.is_empty() {
			return Ok(None);
		}
		Ok(Some(res))
	}

	// Returns all values for the `Set-Cookie` header.
	#[qjs(rename = "getSetCookie")]
	pub fn get_set_cookie(&self) -> Vec<String> {
		// This should always be a correct cookie;
		let key = HeaderName::from_str("set-cookie").expect("valid header name");
		self.inner.get_all(key).iter().map(|x| header_value_to_string(x).into_owned()).collect()
	}

	// Checks to see if the header set contains a header
	pub fn has(&self, ctx: Ctx<'_>, key: String) -> Result<bool> {
		// Process and check the header name is valid
		let key =
			HeaderName::from_str(&key).map_err(|e| Exception::throw_type(&ctx, &format!("{e}")))?;
		// Check if the header entry exists
		Ok(self.inner.contains_key(&key))
	}

	// Returns all header keys contained in the header set
	pub fn keys(&self) -> Vec<String> {
		// Spec returns a Headers Iterator; we eagerly collect into a Vec because
		// exposing a true JS iterator from `#[qjs]` would need a hand-rolled
		// iterator class. JS array iteration still covers the common
		// `for (const k of headers.keys())` usage.
		self.inner.keys().map(|v| v.as_str().to_owned()).collect::<Vec<String>>()
	}

	// Sets a new value or adds a header to the header set
	pub fn set(&mut self, ctx: Ctx<'_>, key: String, val: String) -> Result<()> {
		// Process and check the header name is valid
		let key = HeaderName::from_str(&key)
			.map_err(|e| Exception::throw_type(&ctx, &format!("Invalid header name: {e}")))?;
		// Process and check the header name is valid
		let val = HeaderValue::from_str(&val)
			.map_err(|e| Exception::throw_type(&ctx, &format!("Invalid header value: {e}")))?;
		// Insert and overwrite the header entry
		self.inner.insert(key, val);
		// Everything ok
		Ok(())
	}

	// Returns all header values contained in the header set
	pub fn values(&self) -> Vec<String> {
		let mut res = Vec::<String>::with_capacity(self.inner.len());

		let mut pref = None;
		for (k, v) in self.inner.iter() {
			if Some(k) == pref {
				let ent = res.last_mut().expect("last element exists");
				ent.push_str(", ");
				ent.push_str(&header_value_to_string(v))
			} else {
				pref = Some(k);
				res.push(header_value_to_string(v).into_owned());
			}
		}

		res
	}
}

impl Headers {
	pub fn from_map(map: HeaderMap) -> Self {
		Self {
			inner: map,
		}
	}

	pub fn new_empty() -> Self {
		Self::from_map(HeaderMap::new())
	}

	pub fn new_inner<'js>(ctx: &Ctx<'js>, val: Value<'js>) -> Result<Self> {
		static INVALID_ERROR: &str = "Headers constructor: init was neither sequence<sequence<ByteString>> or record<ByteString, ByteString>";
		let mut res = Self::new_empty();

		// TODO Set and Map,
		if let Some(array) = val.as_array() {
			// a sequence<sequence<String>>;
			for v in array.iter::<Array>() {
				let v = match v {
					Ok(x) => x,
					Err(e) => {
						if e.is_from_js() {
							return Err(Exception::throw_type(ctx, INVALID_ERROR));
						}
						return Err(e);
					}
				};
				let key = match v.get::<Coerced<String>>(0) {
					Ok(x) => x,
					Err(e) => {
						if e.is_from_js() {
							return Err(Exception::throw_type(ctx, INVALID_ERROR));
						}
						return Err(e);
					}
				};
				let value = match v.get::<Coerced<String>>(1) {
					Ok(x) => x,
					Err(e) => {
						if e.is_from_js() {
							return Err(Exception::throw_type(ctx, INVALID_ERROR));
						}
						return Err(e);
					}
				};
				res.append_inner(ctx, &key, &value)?;
			}
		} else if let Some(obj) = val.as_object() {
			// a record<String,String>;
			for prop in obj.props::<String, Coerced<String>>() {
				let (key, value) = match prop {
					Ok(x) => x,
					Err(e) => {
						if e.is_from_js() {
							return Err(Exception::throw_type(ctx, INVALID_ERROR));
						}
						return Err(e);
					}
				};
				res.append_inner(ctx, &key, &value.0)?;
			}
		} else {
			return Err(Exception::throw_type(ctx, INVALID_ERROR));
		}

		Ok(res)
	}

	fn append_inner(&mut self, ctx: &Ctx<'_>, key: &str, val: &str) -> Result<()> {
		// Unsure what to do exactly here.
		// Spec dictates normalizing string before adding it as a header value, i.e.
		// removing any leading and trailing whitespace:
		// [`https://fetch.spec.whatwg.org/#concept-header-value-normalize`]
		// But non of the platforms I tested, normalize, instead they throw an error
		// with `Invalid header value`. I'll chose to just do what the platforms do.

		let key = match HeaderName::from_bytes(key.as_bytes()) {
			Ok(x) => x,
			Err(e) => {
				return Err(Exception::throw_type(
					ctx,
					&format!("invalid header name `{key}`: {e}"),
				));
			}
		};
		let val = match HeaderValue::from_bytes(val.as_bytes()) {
			Ok(x) => x,
			Err(e) => {
				return Err(Exception::throw_type(
					ctx,
					&format!("invalid header value `{val}`: {e}"),
				));
			}
		};

		self.inner.append(key, val);

		Ok(())
	}
}

#[cfg(test)]
mod test {
	use js::CatchResultExt;
	use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

	use super::Headers;
	use crate::fnc::script::fetch::test::create_test_context;

	/// Builds a `Headers` the way a response does — from a real `HeaderMap`
	/// rather than from script-authored strings.
	fn headers_with_value(name: &str, value: HeaderValue) -> Headers {
		let mut map = HeaderMap::new();
		map.insert(HeaderName::from_bytes(name.as_bytes()).expect("valid header name"), value);
		Headers::from_map(map)
	}

	// `Headers::from_map` is fed the `HeaderMap` of an actual outbound
	// response, so its values are whatever the remote server sent. HTTP header
	// values are opaque bytes and `HeaderValue::to_str` succeeds only for
	// visible ASCII, so the accessors must not assume it succeeds:
	// under the release profile's `panic = "abort"` a panic here takes the
	// whole process down, reachable by any allowed endpoint simply choosing to
	// return such a header.
	#[test]
	fn response_accessors_tolerate_non_utf8_header_values() {
		// The crux: `from_bytes` accepts bytes that `to_str` then rejects.
		let invalid = HeaderValue::from_bytes(&[0xff, 0xfe]).expect("from_bytes accepts raw bytes");
		assert!(invalid.to_str().is_err(), "[0xff, 0xfe] must not be valid UTF-8");

		// Every response-reachable accessor must return rather than panic.
		let headers = headers_with_value("x-invalid", invalid.clone());
		assert_eq!(headers.entries().len(), 1, "entries must yield the header");
		assert_eq!(headers.values().len(), 1, "values must yield the header");
		let got = headers.keys();
		assert_eq!(got, vec!["x-invalid".to_owned()]);

		let cookies = headers_with_value("set-cookie", invalid);
		assert_eq!(cookies.get_set_cookie().len(), 1, "getSetCookie must yield the cookie");
		assert_eq!(cookies.entries().len(), 1);
	}

	// A value that decodes cleanly must keep its exact representation: the
	// fallback applies only to values `to_str` rejects.
	#[test]
	fn valid_header_values_are_unchanged() {
		let headers = headers_with_value("x-valid", HeaderValue::from_static("plain-ascii"));
		let entries = headers.entries();
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].0.1, "plain-ascii");
		assert_eq!(headers.values(), vec!["plain-ascii".to_owned()]);

		// Valid multi-byte UTF-8 must survive intact rather than being decoded
		// byte-wise, which is what a blanket isomorphic decode would do.
		let utf8 = HeaderValue::from_bytes("café".as_bytes()).expect("valid utf-8 bytes");
		let headers = headers_with_value("x-utf8", utf8);
		assert_eq!(headers.entries()[0].0.1, "café");
	}

	#[tokio::test]
	async fn basic_headers_use() {
		create_test_context!(ctx => {
			ctx.eval::<(),_>(r#"
				let headers = new Headers([
					["a","b"],
					["a","c"],
					["d","e"],
				]);
				assert(headers.has("a"));
				assert(headers.has("d"));
				assert(headers.has("d"));

				let keys = [];
				for(const key of headers.keys()){
					keys.push(key);
				}
				assert.seq(keys[0], "a");
				assert.seq(keys[1], "d");
				assert.seq(headers.get("a"), "b, c");

				let values = [];
				for(const v of headers.values()){
					values.push(v);
				}
				assert.seq(values[0], "b, c");
				assert.seq(values[1], "e");

				headers.set("a","f");
				assert.seq(headers.get("a"), "f");
				assert.seq(headers.get("A"), "f");
				headers.append("a","g");
				assert.seq(headers.get("a"), "f, g");
				headers.delete("a");
				assert(!headers.has("a"));

				headers.set("Set-Cookie","somecookie");
				let cookies = headers.getSetCookie();
				assert.seq(cookies.length,1);
				assert.seq(cookies[0],"somecookie");
				headers.append("sEt-cOoKiE","memecookie");
				cookies = headers.getSetCookie();
				assert.seq(cookies.length,2);
				assert.seq(cookies[0],"somecookie");
				assert.seq(cookies[1],"memecookie");

				headers = new Headers({
					"f": "g",
					"h": "j",
				});
				assert.seq(headers.get("f"), "g");
				assert.seq(headers.get("h"), "j");
			"#).catch(&ctx).unwrap();
		})
		.await
	}
}
