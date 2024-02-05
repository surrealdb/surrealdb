use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;

#[cfg(not(feature = "http"))]
pub async fn head(_: &Context<'_>, (_, _): (Value, Option<Value>)) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn get(_: &Context<'_>, (_, _): (Value, Option<Value>)) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn put(
	_: &Context<'_>,
	(_, _, _): (Value, Option<Value>, Option<Value>),
) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn post(
	_: &Context<'_>,
	(_, _, _): (Value, Option<Value>, Option<Value>),
) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn patch(
	_: &Context<'_>,
	(_, _, _): (Value, Option<Value>, Option<Value>),
) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(not(feature = "http"))]
pub async fn delete(_: &Context<'_>, (_, _): (Value, Option<Value>)) -> Result<Value, Error> {
	Err(Error::HttpDisabled)
}

#[cfg(feature = "http")]
fn try_as_uri(fn_name: &str, value: Value) -> Result<crate::sql::Strand, Error> {
	match value {
		// Pre-check URI.
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
) -> Result<Option<crate::sql::Object>, Error> {
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
pub async fn head(ctx: &Context<'_>, (uri, opts): (Value, Option<Value>)) -> Result<Value, Error> {
	let uri = try_as_uri("http::head", uri)?;
	let opts = try_as_opts("http::head", "The second argument should be an object.", opts)?;
	crate::fnc::util::http::head(ctx, uri, opts).await
}

#[cfg(feature = "http")]
pub async fn get(ctx: &Context<'_>, (uri, opts): (Value, Option<Value>)) -> Result<Value, Error> {
	let uri = try_as_uri("http::get", uri)?;
	let opts = try_as_opts("http::get", "The second argument should be an object.", opts)?;
	crate::fnc::util::http::get(ctx, uri, opts).await
}

#[cfg(feature = "http")]
pub async fn put(
	ctx: &Context<'_>,
	(uri, body, opts): (Value, Option<Value>, Option<Value>),
) -> Result<Value, Error> {
	let uri = try_as_uri("http::put", uri)?;
	let opts = try_as_opts("http::put", "The third argument should be an object.", opts)?;
	crate::fnc::util::http::put(ctx, uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn post(
	ctx: &Context<'_>,
	(uri, body, opts): (Value, Option<Value>, Option<Value>),
) -> Result<Value, Error> {
	let uri = try_as_uri("http::post", uri)?;
	let opts = try_as_opts("http::post", "The third argument should be an object.", opts)?;
	crate::fnc::util::http::post(ctx, uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn patch(
	ctx: &Context<'_>,
	(uri, body, opts): (Value, Option<Value>, Option<Value>),
) -> Result<Value, Error> {
	let uri = try_as_uri("http::patch", uri)?;
	let opts = try_as_opts("http::patch", "The third argument should be an object.", opts)?;
	crate::fnc::util::http::patch(ctx, uri, body.unwrap_or(Value::Null), opts).await
}

#[cfg(feature = "http")]
pub async fn delete(
	ctx: &Context<'_>,
	(uri, opts): (Value, Option<Value>),
) -> Result<Value, Error> {
	let uri = try_as_uri("http::delete", uri)?;
	let opts = try_as_opts("http::delete", "The second argument should be an object.", opts)?;
	crate::fnc::util::http::delete(ctx, uri, opts).await
}
