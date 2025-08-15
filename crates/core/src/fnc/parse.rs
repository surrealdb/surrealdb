pub mod email {

	use addr::email::Host;
	use anyhow::Result;

	use crate::val::Value;

	pub fn host((string,): (String,)) -> Result<Value> {
		// Parse the email address
		Ok(match addr::parse_email_address(&string) {
			// Return the host part
			Ok(v) => match v.host() {
				Host::Domain(name) => name.as_str().into(),
				Host::IpAddr(ip_addr) => ip_addr.to_string().into(),
			},
			Err(_) => Value::None,
		})
	}

	pub fn user((string,): (String,)) -> Result<Value> {
		// Parse the email address
		Ok(match addr::parse_email_address(&string) {
			// Return the user part
			Ok(v) => v.user().into(),
			Err(_) => Value::None,
		})
	}

	#[cfg(test)]
	mod tests {
		use super::*;

		#[test]
		fn host() {
			let input = (String::from("john.doe@example.com"),);
			let value = super::host(input).unwrap();
			assert_eq!(value, Value::from("example.com"));
		}

		#[test]
		fn user() {
			let input = (String::from("john.doe@example.com"),);
			let value = super::user(input).unwrap();
			assert_eq!(value, Value::from("john.doe"));
		}
	}
}

pub mod url {

	use anyhow::Result;
	use url::Url;

	use crate::val::Value;

	pub fn domain((string,): (String,)) -> Result<Value> {
		match Url::parse(&string) {
			Ok(v) => match v.domain() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn fragment((string,): (String,)) -> Result<Value> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => match v.fragment() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn host((string,): (String,)) -> Result<Value> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => match v.host_str() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn path((string,): (String,)) -> Result<Value> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => Ok(v.path().into()),
			Err(_) => Ok(Value::None),
		}
	}

	pub fn port((string,): (String,)) -> Result<Value> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => match v.port_or_known_default() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn query((string,): (String,)) -> Result<Value> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => match v.query() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn scheme((string,): (String,)) -> Result<Value> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => Ok(v.scheme().into()),
			Err(_) => Ok(Value::None),
		}
	}

	#[cfg(test)]
	mod tests {
		use crate::val::Value;

		#[test]
		fn port_default_port_specified() {
			let value = super::port(("http://www.google.com:80".to_string(),)).unwrap();
			assert_eq!(value, Value::from(80));
		}

		#[test]
		fn port_nondefault_port_specified() {
			let value = super::port(("http://www.google.com:8080".to_string(),)).unwrap();
			assert_eq!(value, Value::from(8080));
		}

		#[test]
		fn port_no_port_specified() {
			let value = super::port(("http://www.google.com".to_string(),)).unwrap();
			assert_eq!(value, Value::from(80));
		}

		#[test]
		fn port_no_scheme_no_port_specified() {
			let value = super::port(("www.google.com".to_string(),)).unwrap();
			assert_eq!(value, Value::None);
		}
	}
}
