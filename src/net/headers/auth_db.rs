use axum_extra::headers;
use axum_extra::headers::Header;
use http::HeaderName;
use http::HeaderValue;
use surrealdb::headers::AUTH_DB;

/// Typed header implementation for the `surreal-auth-db` header.
/// It's used to specify the database to use for the basic authentication.
pub struct SurrealAuthDatabase(String);

impl Header for SurrealAuthDatabase {
	fn name() -> &'static HeaderName {
		&AUTH_DB
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?;
		let value = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealAuthDatabase(value))
	}

	fn encode<E>(&self, values: &mut E)
	where
		E: Extend<HeaderValue>,
	{
		values.extend(std::iter::once(self.into()));
	}
}

impl std::ops::Deref for SurrealAuthDatabase {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<SurrealAuthDatabase> for HeaderValue {
	fn from(value: SurrealAuthDatabase) -> Self {
		HeaderValue::from(&value)
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<&SurrealAuthDatabase> for HeaderValue {
	fn from(value: &SurrealAuthDatabase) -> Self {
		HeaderValue::from_str(value.0.as_str()).unwrap()
	}
}
