pub mod email {

	use crate::ctx::Context;
	use crate::err::Error;
	use crate::sql::value::Value;
	use once_cell::sync::Lazy;
	use regex::Regex;

	#[rustfmt::skip] static USER_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(?i)[a-z0-9.!#$%&'*+/=?^_`{|}~-]+\z").unwrap());
	#[rustfmt::skip] static HOST_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$",).unwrap());

	pub fn domain(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		// Convert to a String
		let val = args.remove(0).as_string();
		// Check if value is empty
		if val.is_empty() {
			return Ok(Value::None);
		}
		// Ensure the value contains @
		if !val.contains('@') {
			return Ok(Value::None);
		}
		// Reverse split the value by @
		let parts: Vec<&str> = val.rsplitn(2, '@').collect();
		// Check the first part matches
		if !USER_RE.is_match(parts[1]) {
			return Ok(Value::None);
		}
		// Check the second part matches
		if !HOST_RE.is_match(parts[0]) {
			return Ok(Value::None);
		}
		// Return the domain
		Ok(parts[0].into())
	}

	pub fn user(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		// Convert to a String
		let val = args.remove(0).as_string();
		// Check if value is empty
		if val.is_empty() {
			return Ok(Value::None);
		}
		// Ensure the value contains @
		if !val.contains('@') {
			return Ok(Value::None);
		}
		// Reverse split the value by @
		let parts: Vec<&str> = val.rsplitn(2, '@').collect();
		// Check the first part matches
		if !USER_RE.is_match(parts[1]) {
			return Ok(Value::None);
		}
		// Check the second part matches
		if !HOST_RE.is_match(parts[0]) {
			return Ok(Value::None);
		}
		// Return the domain
		Ok(parts[1].into())
	}
}

pub mod url {

	use crate::ctx::Context;
	use crate::err::Error;
	use crate::sql::value::Value;
	use url::Url;

	pub fn domain(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		// Convert to a String
		let val = args.remove(0).as_string();
		// Parse the URL
		match Url::parse(&val) {
			Ok(v) => match v.domain() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn fragment(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		// Convert to a String
		let val = args.remove(0).as_string();
		// Parse the URL
		match Url::parse(&val) {
			Ok(v) => match v.fragment() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn host(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		// Convert to a String
		let val = args.remove(0).as_string();
		// Parse the URL
		match Url::parse(&val) {
			Ok(v) => match v.host_str() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn path(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		// Convert to a String
		let val = args.remove(0).as_string();
		// Parse the URL
		match Url::parse(&val) {
			Ok(v) => Ok(v.path().into()),
			Err(_) => Ok(Value::None),
		}
	}

	pub fn port(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		// Convert to a String
		let val = args.remove(0).as_string();
		// Parse the URL
		match Url::parse(&val) {
			Ok(v) => match v.port() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn query(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		// Convert to a String
		let val = args.remove(0).as_string();
		// Parse the URL
		match Url::parse(&val) {
			Ok(v) => match v.query() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}
}
