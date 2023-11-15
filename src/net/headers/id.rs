use axum::headers;
use axum::headers::Header;
use http::HeaderName;
use http::HeaderValue;
use surrealdb::headers::ID;

/// Typed header implementation for the id header.
/// It's used to specify the session id.
pub struct SurrealId(String);

impl Header for SurrealId {
	fn name() -> &'static HeaderName {
		&ID
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?;
		let value = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealId(value))
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
		&self.0
	}
}

impl From<SurrealId> for HeaderValue {
	fn from(value: SurrealId) -> Self {
		HeaderValue::from(&value)
	}
}

impl From<&SurrealId> for HeaderValue {
	fn from(value: &SurrealId) -> Self {
		HeaderValue::from_str(value.0.as_str()).unwrap()
	}
}

//
// Legacy header
//
static ID_LEGACY_HEADER: HeaderName = HeaderName::from_static("id");
pub struct SurrealIdLegacy(String);

impl Header for SurrealIdLegacy {
	fn name() -> &'static HeaderName {
		&ID_LEGACY_HEADER
	}

	fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
	where
		I: Iterator<Item = &'i HeaderValue>,
	{
		let value = values.next().ok_or_else(headers::Error::invalid)?;
		let value = value.to_str().map_err(|_| headers::Error::invalid())?.to_string();

		Ok(SurrealIdLegacy(value))
	}

	fn encode<E>(&self, values: &mut E)
	where
		E: Extend<HeaderValue>,
	{
		values.extend(std::iter::once(self.into()));
	}
}

impl std::ops::Deref for SurrealIdLegacy {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<SurrealIdLegacy> for HeaderValue {
	fn from(value: SurrealIdLegacy) -> Self {
		HeaderValue::from(&value)
	}
}

impl From<&SurrealIdLegacy> for HeaderValue {
	fn from(value: &SurrealIdLegacy) -> Self {
		HeaderValue::from_str(value.0.as_str()).unwrap()
	}
}
