use axum_extra::headers;
use axum_extra::headers::Header;
use http::{HeaderName, HeaderValue};
use surrealdb::headers::ID;

/// Typed header implementation for the id header.
/// It's used to specify the session id.
pub struct SurrealId(HeaderValue, String);

impl Header for SurrealId {
	fn name() -> &'static HeaderName {
		&ID
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?.clone();
		let string = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealId(value, string))
	}

	fn encode<E>(&self, values: &mut E)
	where
		E: Extend<HeaderValue>,
	{
		values.extend(std::iter::once(self.into()));
	}
}

impl std::ops::Deref for SurrealId {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.1
	}
}

impl From<SurrealId> for HeaderValue {
	fn from(value: SurrealId) -> Self {
		HeaderValue::from(&value)
	}
}

impl From<&SurrealId> for HeaderValue {
	fn from(value: &SurrealId) -> Self {
		value.0.clone()
	}
}
