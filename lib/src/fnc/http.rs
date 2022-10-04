use crate::err::Error;
use crate::sql::value::Value;
use crate::sql::{Object, Strand};

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

#[cfg(feature = "http")]
fn try_as_uri(fn_name: &str, value: Value) -> Result<Strand, Error> {
	match value {
		// Avoid surf crate panic by pre-checking URI.
		Value::Strand(uri) if crate::fnc::util::http::uri_is_valid(&uri) => Ok(uri),
		_ => Err(Error::InvalidArguments {
			name: fn_name.to_owned(),
			// Assumption is that URI is first argument.
			message: String::from("The first argument should be a string containing a valid URI."),
		}),
	}
}

#[cfg(feature = "http")]
fn try_as_opts(
	fn_name: &str,
	error_message: &str,
	value: Option<Value>,
) -> Result<Option<Object>, Error> {
	match value {
		Some(Value::Object(opts)) => Ok(Some(opts)),
		None => Ok(None),
		Some(_) => Err(Error::InvalidArguments {
			name: fn_name.to_owned(),
			message: error_message.to_owned(),
		}),
	}
}

#[cfg(feature = "http")]
pub async fn head((uri, opts): (Value, Option<Value>)) -> Result<Value, Error> {
	let uri = try_as_uri("http::head", uri)?;
	let opts = try_as_opts("http::head", "The second argument should be an object.", opts)?;
	crate::fnc::util::http::head(uri, opts).await
}

#[cfg(feature = "http")]
pub async fn get((uri, opts): (Value, Option<Value>)) -> Result<Value, Error> {
	let uri = try_as_uri("http::get", uri)?;
	let opts = try_as_opts("http::get", "The second argument should be an object.", opts)?;
	crate::fnc::util::http::get(uri, opts).await
}

#[cfg(feature = "http")]
pub async fn put((uri, body, opts): (Value, Option<Value>, Option<Value>)) -> Result<Value, Error> {
	let uri = try_as_uri("http::put", uri)?;
	let opts = try_as_opts("http::put", "The third argument should be an object.", opts)?;
	crate::fnc::util::http::put(uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn post(
	(uri, body, opts): (Value, Option<Value>, Option<Value>),
) -> Result<Value, Error> {
	let uri = try_as_uri("http::post", uri)?;
	let opts = try_as_opts("http::post", "The third argument should be an object.", opts)?;
	crate::fnc::util::http::post(uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn patch(
	(uri, body, opts): (Value, Option<Value>, Option<Value>),
) -> Result<Value, Error> {
	let uri = try_as_uri("http::patch", uri)?;
	let opts = try_as_opts("http::patch", "The third argument should be an object.", opts)?;
	crate::fnc::util::http::patch(uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn delete((uri, opts): (Value, Option<Value>)) -> Result<Value, Error> {
	let uri = try_as_uri("http::delete", uri)?;
	let opts = try_as_opts("http::delete", "The second argument should be an object.", opts)?;
	crate::fnc::util::http::delete(uri, opts).await
}
