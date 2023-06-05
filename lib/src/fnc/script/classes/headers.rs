#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod headers {

	use js::function::Rest;
	use js::Value;
	use reqwest::header::HeaderName;
	use std::cell::RefCell;
	use std::collections::HashMap;
	use std::str::FromStr;

	#[derive(Clone)]
	#[quickjs(cloneable)]
	#[allow(dead_code)]
	pub struct Headers {
		pub(crate) inner: RefCell<HashMap<HeaderName, Vec<String>>>,
	}

	impl Headers {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new(args: Rest<Value>) -> Self {
			Self {
				inner: RefCell::new(HashMap::new()),
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
		pub fn append(
			&self,
			ctx: js::Ctx<'_>,
			key: String,
			val: String,
			args: Rest<Value>,
		) -> js::Result<()> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key).map_err(|e| throw!(ctx, e))?;
			// Insert and overwrite the header entry
			self.inner.borrow_mut().entry(key).or_insert_with(Vec::new).push(val);
			// Everything ok
			Ok(())
		}

		// Deletes a header from the header set
		pub fn delete(&self, ctx: js::Ctx<'_>, key: String, args: Rest<Value>) -> js::Result<()> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key).map_err(|e| throw!(ctx, e))?;
			// Remove the header entry from the map
			self.inner.borrow_mut().remove(&key);
			// Everything ok
			Ok(())
		}

		// Returns all header entries in the header set
		pub fn entries(&self, args: Rest<Value>) -> Vec<(String, String)> {
			self.inner
				.borrow()
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
		pub fn get(
			&self,
			ctx: js::Ctx<'_>,
			key: String,
			args: Rest<Value>,
		) -> js::Result<Option<String>> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key).map_err(|e| throw!(ctx, e))?;
			// Convert the header values to strings
			Ok(self
				.inner
				.borrow()
				.get(&key)
				.map(|v| v.iter().map(|v| v.as_str()).collect::<Vec<&str>>().join(",")))
		}

		// Checks to see if the header set contains a header
		pub fn has(&self, ctx: js::Ctx<'_>, key: String, args: Rest<Value>) -> js::Result<bool> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key).map_err(|e| throw!(ctx, e))?;
			// Check if the header entry exists
			Ok(self.inner.borrow().contains_key(&key))
		}

		// Returns all header keys contained in the header set
		pub fn keys(&self, args: Rest<Value>) -> Vec<String> {
			self.inner.borrow().keys().map(|v| v.as_str().to_owned()).collect::<Vec<String>>()
		}

		// Sets a new value or adds a header to the header set
		pub fn set(
			&self,
			ctx: js::Ctx<'_>,
			key: String,
			val: String,
			args: Rest<Value>,
		) -> js::Result<()> {
			// Process and check the header name is valid
			let key = HeaderName::from_str(&key).map_err(|e| throw!(ctx, e))?;
			// Insert and overwrite the header entry
			self.inner.borrow_mut().insert(key, vec![val]);
			// Everything ok
			Ok(())
		}

		// Returns all header values contained in the header set
		pub fn values(&self, args: Rest<Value>) -> Vec<String> {
			self.inner
				.borrow()
				.values()
				.map(|v| v.iter().map(|v| v.as_str()).collect::<Vec<&str>>().join(","))
				.collect::<Vec<String>>()
		}
	}
}
