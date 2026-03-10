use axum_extra::headers;
use axum_extra::headers::Header;
use http::{HeaderName, HeaderValue};
use surrealdb::headers::AUTH_NS;

/// Typed header implementation for the `surreal-auth-ns` header.
/// It's used to specify the namespace to use for the basic authentication.
pub struct SurrealAuthNamespace(HeaderValue, String);

impl Header for SurrealAuthNamespace {
	fn name() -> &'static HeaderName {
		&AUTH_NS
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?.clone();
		let string = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealAuthNamespace(value, string))
	}

	fn encode<E>(&self, values: &mut E)
	where
		E: Extend<HeaderValue>,
	{
		values.extend(std::iter::once(self.into()));
	}
}

impl std::ops::Deref for SurrealAuthNamespace {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.1
	}
}

impl From<SurrealAuthNamespace> for HeaderValue {
	fn from(value: SurrealAuthNamespace) -> Self {
		HeaderValue::from(&value)
	}
}

impl From<&SurrealAuthNamespace> for HeaderValue {
	fn from(value: &SurrealAuthNamespace) -> Self {
		value.0.clone()
	}
}
