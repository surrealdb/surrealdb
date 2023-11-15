use axum::headers;
use axum::headers::Header;
use http::HeaderName;
use http::HeaderValue;
use surrealdb::headers::NS;

/// Typed header implementation for the namespace header.
/// It's used to specify the database to use for database operations.
pub struct SurrealNamespace(String);

impl Header for SurrealNamespace {
	fn name() -> &'static HeaderName {
		&NS
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?;
		let value = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealNamespace(value))
	}

	fn encode<E>(&self, values: &mut E)
	where
		E: Extend<HeaderValue>,
	{
		values.extend(std::iter::once(self.into()));
	}
}

impl std::ops::Deref for SurrealNamespace {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<SurrealNamespace> for HeaderValue {
	fn from(value: SurrealNamespace) -> Self {
		HeaderValue::from(&value)
	}
}

impl From<&SurrealNamespace> for HeaderValue {
	fn from(value: &SurrealNamespace) -> Self {
		HeaderValue::from_str(value.0.as_str()).unwrap()
	}
}

//
// Legacy header
//
static NS_LEGACY_HEADER: HeaderName = HeaderName::from_static("ns");

pub struct SurrealNamespaceLegacy(String);

impl Header for SurrealNamespaceLegacy {
	fn name() -> &'static HeaderName {
		&NS_LEGACY_HEADER
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?;
		let value = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealNamespaceLegacy(value))
	}

	fn encode<E>(&self, values: &mut E)
	where
		E: Extend<HeaderValue>,
	{
		values.extend(std::iter::once(self.into()));
	}
}

impl std::ops::Deref for SurrealNamespaceLegacy {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<SurrealNamespaceLegacy> for HeaderValue {
	fn from(value: SurrealNamespaceLegacy) -> Self {
		HeaderValue::from(&value)
	}
}

impl From<&SurrealNamespaceLegacy> for HeaderValue {
	fn from(value: &SurrealNamespaceLegacy) -> Self {
		HeaderValue::from_str(value.0.as_str()).unwrap()
	}
}
