use axum::headers;
use axum::headers::Header;
use http::HeaderName;
use http::HeaderValue;
use surrealdb::headers::DB;

/// Typed header implementation for the database header.
/// It's used to specify the database to use for database operations.
pub struct SurrealDatabase(String);

impl Header for SurrealDatabase {
	fn name() -> &'static HeaderName {
		&DB
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?;
		let value = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealDatabase(value))
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
		&self.0
	}
}

impl From<SurrealDatabase> for HeaderValue {
	fn from(value: SurrealDatabase) -> Self {
		HeaderValue::from(&value)
	}
}

impl From<&SurrealDatabase> for HeaderValue {
	fn from(value: &SurrealDatabase) -> Self {
		HeaderValue::from_str(value.0.as_str()).unwrap()
	}
}

//
// Legacy header
//
static DB_LEGACY_HEADER: HeaderName = HeaderName::from_static("db");

pub struct SurrealDatabaseLegacy(String);

impl Header for SurrealDatabaseLegacy {
	fn name() -> &'static HeaderName {
		&DB_LEGACY_HEADER
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?;
		let value = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealDatabaseLegacy(value))
	}

	fn encode<E>(&self, values: &mut E)
	where
		E: Extend<HeaderValue>,
	{
		values.extend(std::iter::once(self.into()));
	}
}

impl std::ops::Deref for SurrealDatabaseLegacy {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<SurrealDatabaseLegacy> for HeaderValue {
	fn from(value: SurrealDatabaseLegacy) -> Self {
		HeaderValue::from(&value)
	}
}

impl From<&SurrealDatabaseLegacy> for HeaderValue {
	fn from(value: &SurrealDatabaseLegacy) -> Self {
		HeaderValue::from_str(value.0.as_str()).unwrap()
	}
}
