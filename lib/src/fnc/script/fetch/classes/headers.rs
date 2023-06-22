//! Headers class implementation

use js::bind;

pub use headers::Headers as HeadersClass;

#[bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
mod headers {
	use std::{cell::RefCell, str::FromStr};

	use js::{function::Rest, prelude::Coerced, Array, Ctx, Exception, Result, Value};
	use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

	#[derive(Clone)]
	#[quickjs(cloneable)]
	#[allow(dead_code)]
	pub struct Headers {
		pub(crate) inner: RefCell<HeaderMap>,
	}

	impl Headers {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new<'js>(ctx: Ctx<'js>, init: Value<'js>, args: Rest<()>) -> Result<Self> {
			Headers::new_inner(ctx, init)
		}

		// ------------------------------
		// Instance methods
		// ------------------------------

		// Convert the object to a string
		pub fn toString(&self, args: Rest<()>) -> String {
			String::from("[object Header]")
		}

		// Adds or appends a new value to a header
		pub fn append(&self, ctx: Ctx<'_>, key: String, val: String, args: Rest<()>) -> Result<()> {
			self.append_inner(ctx, &key, &val)
		}

		// Deletes a header from the header set
		pub fn delete(&self, ctx: Ctx<'_>, key: String, args: Rest<()>) -> Result<()> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key)
				.map_err(|e| Exception::throw_type(ctx, &format!("{e}")))?;
			// Remove the header entry from the map
			self.inner.borrow_mut().remove(&key);
			// Everything ok
			Ok(())
		}

		// Returns all header entries in the header set
		pub fn entries(&self, args: Rest<()>) -> Vec<(String, String)> {
			let lock = self.inner.borrow();
			let mut res = Vec::<(String, String)>::with_capacity(lock.len());

			for (k, v) in lock.iter() {
				let k = k.as_str();
				if Some(k) == res.last().map(|x| x.0.as_str()) {
					let ent = res.last_mut().unwrap();
					ent.1.push_str(", ");
					// Header value came from a string, so it should also be able to be cast back
					// to a string
					ent.1.push_str(v.to_str().unwrap());
				} else {
					res.push((k.to_owned(), v.to_str().unwrap().to_owned()));
				}
			}

			res
		}

		// Returns all values of a header in the header set
		pub fn get(&self, ctx: Ctx<'_>, key: String, args: Rest<()>) -> Result<Option<String>> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key)
				.map_err(|e| Exception::throw_type(ctx, &format!("{e}")))?;
			// Convert the header values to strings
			let lock = self.inner.borrow();
			let all = lock.get_all(&key);

			// Header value came from a string, so it should also be able to be cast back
			// to a string
			let mut res = String::new();
			for (idx, v) in all.iter().enumerate() {
				if idx != 0 {
					res.push_str(", ");
				}
				res.push_str(v.to_str().unwrap());
			}

			if res.is_empty() {
				return Ok(None);
			}
			Ok(Some(res))
		}

		// Returns all values for the `Set-Cookie` header.
		#[quickjs(rename = "getSetCookie")]
		pub fn get_set_cookie(&self, args: Rest<()>) -> Vec<String> {
			// This should always be a correct cookie;
			let key = HeaderName::from_str("set-cookie").unwrap();
			self.inner
				.borrow()
				.get_all(key)
				.iter()
				.map(|x| x.to_str().unwrap().to_owned())
				.collect()
		}

		// Checks to see if the header set contains a header
		pub fn has(&self, ctx: Ctx<'_>, key: String, args: Rest<()>) -> Result<bool> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key)
				.map_err(|e| Exception::throw_type(ctx, &format!("{e}")))?;
			// Check if the header entry exists
			Ok(self.inner.borrow().contains_key(&key))
		}

		// Returns all header keys contained in the header set
		pub fn keys(&self, args: Rest<()>) -> Vec<String> {
			// TODO: Incorrect, should return an iterator but iterators are not supported yet by quickjs
			self.inner.borrow().keys().map(|v| v.as_str().to_owned()).collect::<Vec<String>>()
		}

		// Sets a new value or adds a header to the header set
		pub fn set(&self, ctx: Ctx<'_>, key: String, val: String, args: Rest<()>) -> Result<()> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key)
				.map_err(|e| Exception::throw_type(ctx, &format!("Invalid header name: {e}")))?;
			// Process and check the header name is valid
			let val = HeaderValue::from_str(&val)
				.map_err(|e| Exception::throw_type(ctx, &format!("Invalid header value: {e}")))?;
			// Insert and overwrite the header entry
			self.inner.borrow_mut().insert(key, val);
			// Everything ok
			Ok(())
		}

		// Returns all header values contained in the header set
		pub fn values(&self, args: Rest<()>) -> Vec<String> {
			let lock = self.inner.borrow();
			let mut res = Vec::<String>::with_capacity(lock.len());

			let mut pref = None;
			for (k, v) in lock.iter() {
				if Some(k) == pref {
					let ent = res.last_mut().unwrap();
					ent.push_str(", ");
					ent.push_str(v.to_str().unwrap())
				} else {
					pref = Some(k);
					res.push(v.to_str().unwrap().to_owned());
				}
			}

			res
		}
	}

	#[quickjs(skip)]
	impl Headers {
		pub fn from_map(map: HeaderMap) -> Self {
			Self {
				inner: RefCell::new(map),
			}
		}

		pub fn new_empty() -> Self {
			Self::from_map(HeaderMap::new())
		}

		pub fn new_inner<'js>(ctx: Ctx<'js>, val: Value<'js>) -> Result<Self> {
			static INVALID_ERROR: &str = "Headers constructor: init was neither sequence<sequence<ByteString>> or record<ByteString, ByteString>";
			let res = Self::new_empty();

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

		fn append_inner(&self, ctx: Ctx<'_>, key: &str, val: &str) -> Result<()> {
			// Unsure what to do exactly here.
			// Spec dictates normalizing string before adding it as a header value, i.e. removing
			// any leading and trailing whitespace:
			// [`https://fetch.spec.whatwg.org/#concept-header-value-normalize`]
			// But non of the platforms I tested, normalize, instead they throw an error
			// with `Invalid header value`. I'll chose to just do what the platforms do.

			let key = match HeaderName::from_bytes(key.as_bytes()) {
				Ok(x) => x,
				Err(e) => {
					return Err(Exception::throw_type(
						ctx,
						&format!("invalid header name `{key}`: {e}"),
					))
				}
			};
			let val = match HeaderValue::from_bytes(val.as_bytes()) {
				Ok(x) => x,
				Err(e) => {
					return Err(Exception::throw_type(
						ctx,
						&format!("invalid header value `{val}`: {e}"),
					))
				}
			};

			self.inner.borrow_mut().append(key, val);

			Ok(())
		}
	}
}

#[cfg(test)]
mod test {
	use crate::fnc::script::fetch::test::create_test_context;
	use js::CatchResultExt;

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
			"#).catch(ctx).unwrap();
		})
		.await
	}
}
