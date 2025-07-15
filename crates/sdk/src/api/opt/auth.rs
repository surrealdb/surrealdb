//! Authentication types

use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt;
use surrealdb_core::expr::Value;
use surrealdb_core::iam::AccessMethod;
use surrealdb_core::iam::SignupParams;
use surrealdb_core::protocol::TryFromValue;
use surrealdb_protocol::proto::rpc::v1::SignupRequest;
use surrealdb_protocol::proto::v1::Value as ValueProto;

/// Credentials for authenticating with the server
pub trait IntoAccessCredentials {
	fn into_access_method(self) -> AccessMethod;
}

/// Credentials for the root user
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Root<'a> {
	/// The username of the root user
	#[serde(rename = "user")]
	pub username: &'a str,
	/// The password of the root user
	#[serde(rename = "pass")]
	pub password: &'a str,
}

impl IntoAccessCredentials for Root<'_> {
	fn into_access_method(self) -> AccessMethod {
		AccessMethod::RootUser {
			username: self.username.to_string(),
			password: self.password.to_string(),
		}
	}
}

/// Credentials for the namespace user
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Namespace<'a> {
	/// The namespace the user has access to
	#[serde(rename = "ns")]
	pub namespace: &'a str,
	/// The username of the namespace user
	#[serde(rename = "user")]
	pub username: &'a str,
	/// The password of the namespace user
	#[serde(rename = "pass")]
	pub password: &'a str,
}

impl IntoAccessCredentials for Namespace<'_> {
	fn into_access_method(self) -> AccessMethod {
		AccessMethod::NamespaceUser {
			namespace: self.namespace.to_string(),
			username: self.username.to_string(),
			password: self.password.to_string(),
		}
	}
}

/// Credentials for the database user
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Database<'a> {
	/// The namespace the user has access to
	#[serde(rename = "ns")]
	pub namespace: &'a str,
	/// The database the user has access to
	#[serde(rename = "db")]
	pub database: &'a str,
	/// The username of the database user
	#[serde(rename = "user")]
	pub username: &'a str,
	/// The password of the database user
	#[serde(rename = "pass")]
	pub password: &'a str,
}

impl IntoAccessCredentials for Database<'_> {
	fn into_access_method(self) -> AccessMethod {
		AccessMethod::DatabaseUser {
			namespace: self.namespace.to_string(),
			database: self.database.to_string(),
			username: self.username.to_string(),
			password: self.password.to_string(),
		}
	}
}

/// Credentials for the record user
#[derive(Debug, Serialize)]
pub struct RecordCredentials<'a> {
	/// The namespace the user has access to
	#[serde(rename = "ns")]
	pub namespace: &'a str,
	/// The database the user has access to
	#[serde(rename = "db")]
	pub database: &'a str,
	/// The access method to use for signin and signup
	#[serde(rename = "ac")]
	pub access: &'a str,

	pub params: BTreeMap<String, String>,
}

impl IntoAccessCredentials for RecordCredentials<'_> {
	fn into_access_method(self) -> AccessMethod {
		let key = self.params.get("key").map(|v| v.to_string()).expect("key is required");
		let refresh_token = self.params.get("refresh_token").map(|v| v.to_string());

		AccessMethod::DatabaseAccess {
			namespace: self.namespace.to_string(),
			database: self.database.to_string(),
			access_name: self.access.to_string(),
			key,
			refresh_token,
		}
	}
}

impl<'a> From<RecordCredentials<'a>> for SignupParams {
	/// Converts the `RecordCredentials` into a `SignupParams`.
	fn from(credentials: RecordCredentials<'a>) -> SignupParams {
		SignupParams {
			namespace: credentials.namespace.to_string(),
			database: credentials.database.to_string(),
			access_name: credentials.access.to_string(),
			variables: credentials
				.params
				.into_iter()
				.map(|(k, v)| (k, Value::Strand(v.into())))
				.collect(),
		}
	}
}

impl<'a> From<RecordCredentials<'a>> for SignupRequest {
	fn from(credentials: RecordCredentials<'a>) -> Self {
		SignupRequest {
			namespace: credentials.namespace.to_string(),
			database: credentials.database.to_string(),
			access_name: credentials.access.to_string(),
			variables: Some(
				credentials.params.into_iter().map(|(k, v)| (k, ValueProto::string(v))).collect(),
			),
		}
	}
}

/// A JSON Web Token for authenticating with the server.
///
/// This struct represents a JSON Web Token (JWT) that can be used for authentication purposes.
/// It is important to note that this implementation provide some security measures to
/// protect the token:
/// * the debug implementation just prints `Jwt(REDACTED)`,
/// * `Display` is not implemented so you can't call `.to_string()` on it
///
/// You can still have access to the token string using either
/// [`as_insecure_token`](Jwt::as_insecure_token) or [`into_insecure_token`](Jwt::into_insecure_token) functions.
/// However, you should take care to ensure that only authorized users have access to the JWT.
/// For example:
/// * it can be stored in a secure cookie,
/// * stored in a database with restricted access,
/// * or encrypted in conjunction with other encryption mechanisms.
#[derive(Clone, Serialize, Deserialize)]
pub struct Jwt(pub(crate) String);

impl Jwt {
	/// Returns the underlying token string.
	///
	/// ⚠️: It is important to note that the token should be handled securely and protected from unauthorized access.
	pub fn as_insecure_token(&self) -> &str {
		&self.0
	}

	/// Returns the underlying token string.
	///
	/// ⚠️: It is important to note that the token should be handled securely and protected from unauthorized access.
	pub fn into_insecure_token(self) -> String {
		self.0
	}
}

impl From<String> for Jwt {
	fn from(jwt: String) -> Self {
		Jwt(jwt)
	}
}

impl<'a> From<&'a String> for Jwt {
	fn from(jwt: &'a String) -> Self {
		Jwt(jwt.to_owned())
	}
}

impl<'a> From<&'a str> for Jwt {
	fn from(jwt: &'a str) -> Self {
		Jwt(jwt.to_owned())
	}
}

impl TryFromValue for Jwt {
	fn try_from_value(value: ValueProto) -> anyhow::Result<Self> {
		let jwt = String::try_from_value(value)?;
		Ok(Jwt(jwt))
	}
}

impl TryFrom<ValueProto> for Jwt {
	type Error = anyhow::Error;

	fn try_from(value: ValueProto) -> Result<Self, Self::Error> {
		use surrealdb_protocol::proto::v1::value::Value as ValueInner;
		match value.value {
			Some(ValueInner::String(s)) => Ok(Jwt(s)),
			unexpected => Err(anyhow::anyhow!("Expected a string value, got {:?}", unexpected)),
		}
	}
}

impl fmt::Debug for Jwt {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Jwt(REDACTED)")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn as_insecure_token() {
		let jwt = Jwt("super-long-jwt".to_owned());
		assert_eq!(jwt.as_insecure_token(), "super-long-jwt");
	}

	#[test]
	fn into_insecure_token() {
		let jwt = Jwt("super-long-jwt".to_owned());
		assert_eq!(jwt.into_insecure_token(), "super-long-jwt");
	}
}
