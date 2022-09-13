use crate::err::Error;
use crate::sql::value::Value;
use crate::sql::Strand;

#[cfg(not(feature = "http"))]
pub async fn head((_, _): (Value, Option<Value>)) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn get((_, _): (Value, Option<Value>)) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn put((_, _, _): (Value, Option<Value>, Option<Value>)) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn post((_, _, _): (Value, Option<Value>, Option<Value>)) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn patch((_, _, _): (Value, Option<Value>, Option<Value>)) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn delete((_, _): (Value, Option<Value>)) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

fn try_as_uri(fn_name: &str, value: Value) -> Result<Strand, Error> {
	match value {
		Value::Strand(uri) => Ok(uri),
		_ => Err(Error::InvalidArguments {
			name: fn_name.to_owned(),
			// Assumption is that URI is first argument.
			message: String::from("The first argument should be a string."),
		}),
	}
}

#[cfg(feature = "http")]
pub async fn head((uri, opts): (Value, Option<Value>)) -> Result<Value, Error> {
	let uri = try_as_uri("http::head", uri)?;

	let opts = match opts {
		Some(Value::Object(opts)) => Some(opts),
		None => None,
		Some(_) => {
			return Err(Error::InvalidArguments {
				name: String::from("http::head"),
				message: String::from("The second argument should be an object."),
			})
		}
	};

	crate::fnc::util::http::head(uri, opts).await
}

#[cfg(feature = "http")]
pub async fn get((uri, opts): (Value, Option<Value>)) -> Result<Value, Error> {
	let uri = try_as_uri("http::get", uri)?;

	let opts = match opts {
		Some(Value::Object(opts)) => Some(opts),
		None => None,
		Some(_) => {
			return Err(Error::InvalidArguments {
				name: String::from("http::get"),
				message: String::from("The second argument should be an object."),
			})
		}
	};

	crate::fnc::util::http::get(uri, opts).await
}

#[cfg(feature = "http")]
pub async fn put((uri, body, opts): (Value, Option<Value>, Option<Value>)) -> Result<Value, Error> {
	let uri = try_as_uri("http::put", uri)?;

	let opts = match opts {
		Some(Value::Object(opts)) => Some(opts),
		None => None,
		Some(_) => {
			return Err(Error::InvalidArguments {
				name: String::from("http::put"),
				message: String::from("The third argument should be an object."),
			})
		}
	};

	crate::fnc::util::http::put(uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn post(
	(uri, body, opts): (Value, Option<Value>, Option<Value>),
) -> Result<Value, Error> {
	let uri = try_as_uri("http::post", uri)?;

	let opts = match opts {
		Some(Value::Object(opts)) => Some(opts),
		None => None,
		Some(_) => {
			return Err(Error::InvalidArguments {
				name: String::from("http::post"),
				message: String::from("The third argument should be an object."),
			})
		}
	};

	crate::fnc::util::http::post(uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn patch(
	(uri, body, opts): (Value, Option<Value>, Option<Value>),
) -> Result<Value, Error> {
	let uri = try_as_uri("http::patch", uri)?;

	let opts = match opts {
		Some(Value::Object(opts)) => Some(opts),
		None => None,
		Some(_) => {
			return Err(Error::InvalidArguments {
				name: String::from("http::patch"),
				message: String::from("The third argument should be an object."),
			})
		}
	};

	crate::fnc::util::http::patch(uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn delete((uri, opts): (Value, Option<Value>)) -> Result<Value, Error> {
	let uri = try_as_uri("http::delete", uri)?;

	let opts = match opts {
		Some(Value::Object(opts)) => Some(opts),
		None => None,
		Some(_) => {
			return Err(Error::InvalidArguments {
				name: String::from("http::delete"),
				message: String::from("The second argument should be an object."),
			})
		}
	};

	crate::fnc::util::http::delete(uri, opts).await
}
