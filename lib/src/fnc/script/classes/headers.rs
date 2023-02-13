#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod headers {

	use js::Rest;
	use js::Value;
	use reqwest::header::HeaderName;
	use std::collections::HashMap;
	use std::str::FromStr;

	#[derive(Clone)]
	#[quickjs(class)]
	#[quickjs(cloneable)]
	#[allow(dead_code)]
	pub struct Headers {
		#[quickjs(hide)]
		pub(crate) inner: HashMap<HeaderName, Vec<String>>,
	}

	impl Headers {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new(args: Rest<Value>) -> Self {
			Self {
				inner: HashMap::new(),
			}
		}

		// ------------------------------
		// Instance methods
		// ------------------------------

		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Header]")
		}

		// Adds or appends a new value to a header
		pub fn append(&mut self, key: String, val: String, args: Rest<Value>) -> js::Result<()> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key).map_err(|e| throw!(e))?;
			// Insert and overwrite the header entry
			match self.inner.get_mut(&key) {
				Some(v) => {
					v.push(val);
				}
				None => {
					self.inner.insert(key, vec![val]);
				}
			}
			// Everything ok
			Ok(())
		}

		// Deletes a header from the header set
		pub fn delete(&mut self, key: String, args: Rest<Value>) -> js::Result<()> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key).map_err(|e| throw!(e))?;
			// Remove the header entry from the map
			self.inner.remove(&key);
			// Everything ok
			Ok(())
		}

		// Returns all header entries in the header set
		pub fn entries(&self, args: Rest<Value>) -> Vec<(String, String)> {
			self.inner
				.iter()
				.map(|(k, v)| {
					(
						k.as_str().to_owned(),
						v.iter().map(|v| v.as_str()).collect::<Vec<&str>>().join(","),
					)
				})
				.collect::<Vec<(String, String)>>()
		}

		// Returns all values of a header in the header set
		pub fn get(&self, key: String, args: Rest<Value>) -> js::Result<Option<String>> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key).map_err(|e| throw!(e))?;
			// Convert the header values to strings
			Ok(self
				.inner
				.get(&key)
				.map(|v| v.iter().map(|v| v.as_str()).collect::<Vec<&str>>().join(",")))
		}

		// Checks to see if the header set contains a header
		pub fn has(&self, key: String, args: Rest<Value>) -> js::Result<bool> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key).map_err(|e| throw!(e))?;
			// Check if the header entry exists
			Ok(self.inner.contains_key(&key))
		}

		// Returns all header keys contained in the header set
		pub fn keys(&self, args: Rest<Value>) -> Vec<String> {
			self.inner.keys().map(|v| v.as_str().to_owned()).collect::<Vec<String>>()
		}

		// Sets a new value or adds a header to the header set
		pub fn set(&mut self, key: String, val: String, args: Rest<Value>) -> js::Result<()> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key).map_err(|e| throw!(e))?;
			// Insert and overwrite the header entry
			self.inner.insert(key, vec![val]);
			// Everything ok
			Ok(())
		}

		// Returns all header values contained in the header set
		pub fn values(&self, args: Rest<Value>) -> Vec<String> {
			self.inner
				.values()
				.map(|v| v.iter().map(|v| v.as_str()).collect::<Vec<&str>>().join(","))
				.collect::<Vec<String>>()
		}
	}
}
