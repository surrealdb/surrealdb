pub mod email {

	use crate::err::Error;
	use crate::sql::value::Value;
	use addr::email::Host;

	pub fn host((string,): (String,)) -> Result<Value, Error> {
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

	pub fn user((string,): (String,)) -> Result<Value, Error> {
		// Parse the email address
		Ok(match addr::parse_email_address(&string) {
			// Return the user part
			Ok(v) => v.user().into(),
			Err(_) => Value::None,
		})
	}

	#[cfg(test)]
	mod tests {
		#[test]
		fn host() {
			let input = (String::from("john.doe@example.com"),);
			let value = super::host(input).unwrap();
			assert_eq!(value, "example.com".into());
		}

		#[test]
		fn user() {
			let input = (String::from("john.doe@example.com"),);
			let value = super::user(input).unwrap();
			assert_eq!(value, "john.doe".into());
		}
	}
}

pub mod url {

	use crate::err::Error;
	use crate::sql::value::Value;
	use url::Url;

	pub fn domain((string,): (String,)) -> Result<Value, Error> {
		match Url::parse(&string) {
			Ok(v) => match v.domain() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn fragment((string,): (String,)) -> Result<Value, Error> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => match v.fragment() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn host((string,): (String,)) -> Result<Value, Error> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => match v.host_str() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn path((string,): (String,)) -> Result<Value, Error> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => Ok(v.path().into()),
			Err(_) => Ok(Value::None),
		}
	}

	pub fn port((string,): (String,)) -> Result<Value, Error> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => match v.port_or_known_default() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn query((string,): (String,)) -> Result<Value, Error> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => match v.query() {
				Some(v) => Ok(v.into()),
				None => Ok(Value::None),
			},
			Err(_) => Ok(Value::None),
		}
	}

	pub fn scheme((string,): (String,)) -> Result<Value, Error> {
		// Parse the URL
		match Url::parse(&string) {
			Ok(v) => Ok(v.scheme().into()),
			Err(_) => Ok(Value::None),
		}
	}

	#[cfg(test)]
	mod tests {
		use crate::sql::value::Value;

		#[test]
		fn port_default_port_specified() {
			let value = super::port(("http://www.google.com:80".to_string(),)).unwrap();
			assert_eq!(value, 80.into());
		}

		#[test]
		fn port_nondefault_port_specified() {
			let value = super::port(("http://www.google.com:8080".to_string(),)).unwrap();
			assert_eq!(value, 8080.into());
		}

		#[test]
		fn port_no_port_specified() {
			let value = super::port(("http://www.google.com".to_string(),)).unwrap();
			assert_eq!(value, 80.into());
		}

		#[test]
		fn port_no_scheme_no_port_specified() {
			let value = super::port(("www.google.com".to_string(),)).unwrap();
			assert_eq!(value, Value::None);
		}
	}
}
