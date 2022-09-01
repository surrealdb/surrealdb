pub mod email {

	use crate::ctx::Context;
	use crate::err::Error;
	use crate::sql::value::Value;
	use addr::email::Host;

	pub fn host(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		// Convert to a String
		let val = args.remove(0).as_string();
		// Parse the email address
		match addr::parse_email_address(&val) {
			// Return the host part
			Ok(v) => match v.host() {
				Host::Domain(name) => Ok(name.as_str().into()),
				Host::IpAddr(ip_addr) => Ok(ip_addr.to_string().into()),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn user(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		let val = args.remove(0).as_string();
		// Parse the email address
		match addr::parse_email_address(&val) {
			// Return the user part
			Ok(v) => Ok(v.user().into()),
			Err(_) => Ok(Value::None),
		}
	}

	#[cfg(test)]
	mod tests {
		#[test]
		fn host() {
			let input = vec!["john.doe@example.com".into()];
			let value = super::host(&Default::default(), input).unwrap();
			assert_eq!(value, "example.com".into());
		}

		#[test]
		fn user() {
			let input = vec!["john.doe@example.com".into()];
			let value = super::user(&Default::default(), input).unwrap();
			assert_eq!(value, "john.doe".into());
		}
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
