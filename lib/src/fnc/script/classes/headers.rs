#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(clippy::module_inception)]
pub mod headers {
	use std::collections::HashMap;
	use std::str::FromStr;
	use surf::http::headers::HeaderName;

	use crate::throw_js_exception;

	#[derive(Clone)]
	#[quickjs(class)]
	pub struct Headers {
		#[quickjs(skip)]
		pub(crate) inner: HashMap<HeaderName, Vec<String>>,
	}

	impl Headers {
		#[quickjs(constructor)]
		pub fn new() -> Self {
			Self {
				inner: HashMap::new(),
			}
		}

		pub fn get(&self, name: String) -> Option<String> {
			self.inner
				.get(&HeaderName::from(name.as_str()))
				.map(|v| v.iter().map(|v| v.as_str()).collect::<Vec<&str>>().join(","))
		}

		pub fn set(&mut self, name: String, value: String) -> js::Result<()> {
			let name = HeaderName::from_str(name.as_str()).map_err(|e| throw_js_exception!(e))?;
			// TODO: test ISO-8859-1
			if !value.is_ascii() {
				return Err(throw_js_exception!("String contains non ISO-8859-1 code point."));
			}
			let values = vec![value];
			self.inner.insert(name, values);
			Ok(())
		}

		pub fn append(&mut self, key: String, value: String) -> js::Result<()> {
			let name = HeaderName::from(key.as_str());
			match self.inner.get_mut(&name) {
				Some(headers) => {
					// TODO: test ISO-8859-1
					if !value.is_ascii() {
						return Err(throw_js_exception!(
							"String contains non ISO-8859-1 code point."
						));
					}
					headers.push(value);
				}
				None => {
					self.set(key, value)?;
				}
			}
			Ok(())
		}

		pub fn delete(&mut self, key: String) {
			let name = HeaderName::from(key.as_str());
			self.inner.remove(&name);
		}

		pub fn has(&self, name: String) -> bool {
			self.inner.contains_key(&HeaderName::from(name.as_str()))
		}

		pub fn keys(&self) -> Vec<String> {
			self.inner.keys().map(|name| name.as_str().to_owned()).collect::<Vec<String>>()
		}

		pub fn values(&self) -> Vec<String> {
			self.inner
				.values()
				.into_iter()
				.map(|v| v.iter().map(|v| v.as_str()).collect::<Vec<&str>>().join(","))
				.collect::<Vec<String>>()
		}

		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Headers]")
		}
	}
}

use std::collections::hash_map::IntoIter;
use surf::http::Headers as SurfHeaders;

impl IntoIterator for headers::Headers {
	type Item = (surf::http::headers::HeaderName, Vec<String>);
	type IntoIter = IntoIter<surf::http::headers::HeaderName, Vec<String>>;

	fn into_iter(self) -> Self::IntoIter {
		self.inner.into_iter()
	}
}

impl From<&SurfHeaders> for headers::Headers {
	fn from(sh: &SurfHeaders) -> Self {
		let mut headers = Self::new();
		for (name, values) in sh {
			for value in values {
				headers.append(name.to_string(), value.to_string()).unwrap()
			}
		}
		headers
	}
}
