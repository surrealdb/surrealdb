use axum_extra::headers;
use axum_extra::headers::Header;
use http::{HeaderName, HeaderValue};
use surrealdb::headers::NS;

/// Typed header implementation for the namespace header.
/// It's used to specify the database to use for database operations.
pub struct SurrealNamespace(HeaderValue, String);

impl Header for SurrealNamespace {
	fn name() -> &'static HeaderName {
		&NS
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?.clone();
		let string = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealNamespace(value, string))
	}

	fn encode<E>(&self, values: &mut E)
	where
		E: Extend<HeaderValue>,
	{
		values.extend(std::iter::once(self.0.clone()));
	}
}

impl std::ops::Deref for SurrealNamespace {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.1
	}
}

impl From<SurrealNamespace> for HeaderValue {
	fn from(value: SurrealNamespace) -> Self {
		HeaderValue::from(&value)
	}
}

impl From<&SurrealNamespace> for HeaderValue {
	fn from(value: &SurrealNamespace) -> Self {
		value.0.clone()
	}
}
