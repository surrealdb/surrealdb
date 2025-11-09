use axum_extra::headers;
use axum_extra::headers::Header;
use http::{HeaderName, HeaderValue};
use surrealdb::headers::DB;

/// Typed header implementation for the database header.
/// It's used to specify the database to use for database operations.
pub struct SurrealDatabase(HeaderValue, String);

impl Header for SurrealDatabase {
	fn name() -> &'static HeaderName {
		&DB
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?.clone();
		let string = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealDatabase(value, string))
	}

	fn encode<E>(&self, values: &mut E)
	where
		E: Extend<HeaderValue>,
	{
		values.extend(std::iter::once(self.into()));
	}
}

impl std::ops::Deref for SurrealDatabase {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.1
	}
}

impl From<SurrealDatabase> for HeaderValue {
	fn from(value: SurrealDatabase) -> Self {
		HeaderValue::from(&value)
	}
}

impl From<&SurrealDatabase> for HeaderValue {
	fn from(value: &SurrealDatabase) -> Self {
		value.0.clone()
	}
}
