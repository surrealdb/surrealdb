use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::future::IntoFuture;
use std::marker::PhantomData;

use uuid::Uuid;

use crate::conn::ctx_txn;
use crate::method::{BoxFuture, OnceLockExt};
use crate::types::{Object, SurrealValue, Value, Variables};
use crate::{Connection, Result, Surreal};

/// The query an API call compiles to. The path and the request are bound as
/// variables, so neither is ever spliced into the query text.
const INVOKE_QUERY: &str = "api::invoke($path, $request)";

/// An HTTP method a `DEFINE API` endpoint can be defined for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ApiMethod {
	/// `DELETE`
	Delete,
	/// `GET`
	Get,
	/// `PATCH`
	Patch,
	/// `POST`
	Post,
	/// `PUT`
	Put,
	/// `TRACE`
	Trace,
}

impl ApiMethod {
	/// The lowercase name the server matches a definition's methods against.
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Delete => "delete",
			Self::Get => "get",
			Self::Patch => "patch",
			Self::Post => "post",
			Self::Put => "put",
			Self::Trace => "trace",
		}
	}
}

impl fmt::Display for ApiMethod {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.as_str().to_ascii_uppercase())
	}
}

/// The response of a `DEFINE API` endpoint.
///
/// A response is returned whatever its status, including the `404` the server
/// answers with when no endpoint matches the path and method. Use
/// [`ApiCall::value`] to receive only the body of a successful response.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ApiResponse {
	/// The HTTP status code.
	pub status: u16,
	/// The response headers. A header the endpoint set more than once holds
	/// its values joined with `", "`.
	pub headers: BTreeMap<String, String>,
	/// The response body, or [`Value::None`] when the endpoint set none.
	pub body: Value,
}

impl ApiResponse {
	/// Whether the status is in the `2xx` range.
	pub fn is_success(&self) -> bool {
		(200..300).contains(&self.status)
	}

	/// Decodes the body as `R`.
	pub fn body_as<R: SurrealValue>(self) -> Result<R> {
		R::from_value(self.body).map_err(deserialization_error)
	}

	fn from_value(value: Value) -> Result<Self> {
		let Value::Object(object) = value else {
			return Err(invalid_response("the response is not an object"));
		};
		let mut object = object.into_inner();
		let status = match object.remove("status") {
			Some(value) => u16::from_value(value)
				.map_err(|_| invalid_response("the response status is not a status code"))?,
			None => return Err(invalid_response("the response has no status")),
		};
		let headers = match object.remove("headers") {
			None | Some(Value::None | Value::Null) => BTreeMap::new(),
			Some(Value::Object(headers)) => headers
				.into_inner()
				.into_iter()
				.map(|(name, value)| Ok((name, header_value(value)?)))
				.collect::<Result<_>>()?,
			Some(_) => return Err(invalid_response("the response headers are not an object")),
		};
		let body = object.remove("body").unwrap_or(Value::None);
		Ok(Self {
			status,
			headers,
			body,
		})
	}
}

fn header_value(value: Value) -> Result<String> {
	if let Value::String(value) = value {
		return Ok(value);
	}
	if let Value::Array(values) = value {
		let values = values
			.into_iter()
			.map(|value| {
				if let Value::String(value) = value {
					Ok(value)
				} else {
					Err(invalid_response("a response header value is not a string"))
				}
			})
			.collect::<Result<Vec<_>>>()?;
		return Ok(values.join(", "));
	}
	Err(invalid_response("a response header value is not a string"))
}

fn invalid_response(reason: &str) -> crate::Error {
	crate::Error::serialization(
		format!("Invalid API response: {reason}"),
		crate::types::SerializationError::Deserialization,
	)
}

fn deserialization_error(error: impl fmt::Display) -> crate::Error {
	crate::Error::serialization(
		error.to_string(),
		crate::types::SerializationError::Deserialization,
	)
}

/// Joins a prefix and a path into one absolute path, dropping empty segments,
/// so `("/users", "42/")` and `("users/", "/42")` both name `/users/42`.
fn normalize_path(prefix: &str, path: &str) -> String {
	let segments: Vec<&str> =
		prefix.split('/').chain(path.split('/')).filter(|s| !s.is_empty()).collect();
	format!("/{}", segments.join("/"))
}

/// Invokes the endpoints defined with `DEFINE API` on the selected database.
///
/// Returned by [`Surreal::api`](crate::Surreal::api). Each call compiles to
/// `api::invoke($path, $request)` and runs as a query, so it works over every
/// engine and joins the transaction of the handle it was made from.
///
/// Headers set here are sent with every call made from this handle, and a
/// header set on a call overrides one of the same name set here.
#[derive(Debug, Clone)]
pub struct Api<'r, C: Connection> {
	pub(super) client: &'r Surreal<C>,
	pub(super) txn: Option<Uuid>,
	pub(super) prefix: String,
	pub(super) headers: BTreeMap<String, String>,
}

impl<'r, C> Api<'r, C>
where
	C: Connection,
{
	/// Prefixes the path of every call made from this handle.
	pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
		self.prefix = prefix.into();
		self
	}

	/// Sends a header with every call made from this handle.
	pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
		self.headers.insert(name.into(), value.into());
		self
	}

	/// Calls the endpoint at `path` with `method`.
	pub fn invoke(&self, method: ApiMethod, path: impl AsRef<str>) -> ApiCall<'r, C> {
		ApiCall {
			client: Cow::Borrowed(self.client),
			txn: self.txn,
			path: normalize_path(&self.prefix, path.as_ref()),
			method,
			headers: self.headers.clone(),
			query: BTreeMap::new(),
			body: Value::None,
		}
	}

	/// Calls the `GET` endpoint at `path`.
	pub fn get(&self, path: impl AsRef<str>) -> ApiCall<'r, C> {
		self.invoke(ApiMethod::Get, path)
	}

	/// Calls the `POST` endpoint at `path` with `body`.
	pub fn post(&self, path: impl AsRef<str>, body: impl SurrealValue) -> ApiCall<'r, C> {
		self.invoke(ApiMethod::Post, path).body(body)
	}

	/// Calls the `PUT` endpoint at `path` with `body`.
	pub fn put(&self, path: impl AsRef<str>, body: impl SurrealValue) -> ApiCall<'r, C> {
		self.invoke(ApiMethod::Put, path).body(body)
	}

	/// Calls the `PATCH` endpoint at `path` with `body`.
	pub fn patch(&self, path: impl AsRef<str>, body: impl SurrealValue) -> ApiCall<'r, C> {
		self.invoke(ApiMethod::Patch, path).body(body)
	}

	/// Calls the `DELETE` endpoint at `path`.
	pub fn delete(&self, path: impl AsRef<str>) -> ApiCall<'r, C> {
		self.invoke(ApiMethod::Delete, path)
	}

	/// Calls the `TRACE` endpoint at `path`.
	pub fn trace(&self, path: impl AsRef<str>) -> ApiCall<'r, C> {
		self.invoke(ApiMethod::Trace, path)
	}
}

/// A call to a `DEFINE API` endpoint, made by [`Api`].
///
/// Awaiting it yields the [`ApiResponse`], whatever its status.
/// [`ApiCall::value`] yields only the body, and fails unless the status is in
/// the `2xx` range.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ApiCall<'r, C: Connection> {
	client: Cow<'r, Surreal<C>>,
	txn: Option<Uuid>,
	path: String,
	method: ApiMethod,
	headers: BTreeMap<String, String>,
	query: BTreeMap<String, String>,
	body: Value,
}

impl<'r, C> ApiCall<'r, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> ApiCall<'static, C> {
		ApiCall {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}

	/// Sets a request header, replacing one of the same name.
	pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
		self.headers.insert(name.into(), value.into());
		self
	}

	/// Sets a query string parameter, replacing one of the same name.
	pub fn query(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
		self.query.insert(name.into(), value.into());
		self
	}

	/// Sets the request body.
	pub fn body(mut self, body: impl SurrealValue) -> Self {
		self.body = body.into_value();
		self
	}

	/// Yields the body of the response decoded as `R`, failing unless the
	/// status is in the `2xx` range.
	pub fn value<R>(self) -> ApiValue<'r, C, R> {
		ApiValue {
			call: self,
			response_type: PhantomData,
		}
	}

	/// The variables of the compiled `api::invoke($path, $request)` query.
	fn variables(&self) -> Variables {
		let mut request = Object::new();
		if !self.body.is_none() {
			request.insert("body", self.body.clone());
		}
		request.insert("method", self.method.as_str());
		request.insert("headers", self.headers.clone());
		request.insert("query", self.query.clone());
		let mut variables = Variables::new();
		variables.insert("path", self.path.clone());
		variables.insert("request", request);
		variables
	}
}

impl<'r, Client> IntoFuture for ApiCall<'r, Client>
where
	Client: Connection,
{
	type Output = Result<ApiResponse>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let variables = self.variables();
			let router = self.client.inner.router.extract()?;
			let value = router
				.query_value(
					ctx_txn(self.client.session_id, self.txn),
					Cow::Borrowed(INVOKE_QUERY),
					variables,
				)
				.await?;
			ApiResponse::from_value(value)
		})
	}
}

/// The body of a successful `DEFINE API` response, made by [`ApiCall::value`].
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ApiValue<'r, C: Connection, R> {
	call: ApiCall<'r, C>,
	response_type: PhantomData<R>,
}

impl<C, R> ApiValue<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> ApiValue<'static, C, R> {
		ApiValue {
			call: self.call.into_owned(),
			response_type: PhantomData,
		}
	}
}

impl<'r, Client, R> IntoFuture for ApiValue<'r, Client, R>
where
	Client: Connection,
	R: SurrealValue,
{
	type Output = Result<R>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let method = self.call.method;
		let path = self.call.path.clone();
		let call = self.call.into_future();
		Box::pin(async move {
			let response = call.await?;
			if !response.is_success() {
				return Err(crate::Error::query(
					format!("The {method} {path} request failed with status {}", response.status),
					None,
				));
			}
			response.body_as()
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn paths_are_joined_and_normalised() {
		assert_eq!(normalize_path("", "/users"), "/users");
		assert_eq!(normalize_path("", "users"), "/users");
		assert_eq!(normalize_path("", ""), "/");
		assert_eq!(normalize_path("/users", "42"), "/users/42");
		assert_eq!(normalize_path("users/", "/42/"), "/users/42");
		assert_eq!(normalize_path("/a//b", "c//d"), "/a/b/c/d");
	}

	#[test]
	fn a_response_decodes_with_its_status_headers_and_body() {
		let mut headers = Object::new();
		headers.insert("content-type", "application/json");
		headers.insert("set-cookie", vec!["a=1".to_string(), "b=2".to_string()]);
		let mut object = Object::new();
		object.insert("status", 201);
		object.insert("headers", headers);
		object.insert("body", "created");
		object.insert("request_id", "abc");

		let response = ApiResponse::from_value(Value::Object(object)).unwrap();
		assert_eq!(response.status, 201);
		assert!(response.is_success());
		assert_eq!(response.headers["content-type"], "application/json");
		assert_eq!(response.headers["set-cookie"], "a=1, b=2");
		assert_eq!(response.body_as::<String>().unwrap(), "created");
	}

	#[test]
	fn a_response_without_a_body_has_none() {
		let mut object = Object::new();
		object.insert("status", 404);
		let response = ApiResponse::from_value(Value::Object(object)).unwrap();
		assert_eq!(response.status, 404);
		assert!(!response.is_success());
		assert!(response.headers.is_empty());
		assert_eq!(response.body, Value::None);
	}

	#[test]
	fn a_response_without_a_status_is_refused() {
		assert!(ApiResponse::from_value(Value::Object(Object::new())).is_err());
		assert!(ApiResponse::from_value(Value::String("nope".into())).is_err());
	}
}
