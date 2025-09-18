//! Authentication types

use std::borrow::Cow;
use std::fmt;

use serde::{Deserialize, Serialize};
use surrealdb_types::SurrealValue;

/// A signup action
#[derive(Debug)]
pub struct Signup;

/// A signin action
#[derive(Debug)]
pub struct Signin;

/// Credentials for authenticating with the server
pub trait Credentials<Action, Response>: SurrealValue {}

/// Credentials for the root user
#[derive(Debug, Clone, SurrealValue)]
pub struct Root {
	/// The username of the root user
	#[surreal(rename = "user")]
	pub username: String,
	/// The password of the root user
	#[surreal(rename = "pass")]
	pub password: String,
}

impl Credentials<Signin, Jwt> for Root {}

/// Credentials for the namespace user
#[derive(Debug, Clone, SurrealValue)]
pub struct Namespace {
	/// The namespace the user has access to
	#[surreal(rename = "ns")]
	pub namespace: String,
	/// The username of the namespace user
	#[surreal(rename = "user")]
	pub username: String,
	/// The password of the namespace user
	#[surreal(rename = "pass")]
	pub password: String,
}

impl Credentials<Signin, Jwt> for Namespace {}

/// Credentials for the database user
#[derive(Debug, Clone, SurrealValue)]
pub struct Database {
	/// The namespace the user has access to
	#[surreal(rename = "ns")]
	pub namespace: String,
	/// The database the user has access to
	#[surreal(rename = "db")]
	pub database: String,
	/// The username of the database user
	#[surreal(rename = "user")]
	pub username: String,
	/// The password of the database user
	#[surreal(rename = "pass")]
	pub password: String,
}

impl Credentials<Signin, Jwt> for Database {}

/// Credentials for the record user
#[derive(Debug, SurrealValue)]
pub struct Record<P: SurrealValue> {
	/// The namespace the user has access to
	#[surreal(rename = "ns")]
	pub namespace: String,
	/// The database the user has access to
	#[surreal(rename = "db")]
	pub database: String,
	/// The access method to use for signin and signup
	#[surreal(rename = "ac")]
	pub access: String,
	/// The additional params to use
	#[surreal(flatten)]
	pub params: P,
}

impl<T, P> Credentials<T, Jwt> for Record<P> where P: SurrealValue {}

/// A JSON Web Token for authenticating with the server.
///
/// This struct represents a JSON Web Token (JWT) that can be used for
/// authentication purposes. It is important to note that this implementation
/// provide some security measures to protect the token:
/// * the debug implementation just prints `Jwt(REDACTED)`,
/// * `Display` is not implemented so you can't call `.to_string()` on it
///
/// You can still have access to the token string using either
/// [`as_insecure_token`](Jwt::as_insecure_token) or
/// [`into_insecure_token`](Jwt::into_insecure_token) functions. However, you
/// should take care to ensure that only authorized users have access to the
/// JWT. For example:
/// * it can be stored in a secure cookie,
/// * stored in a database with restricted access,
/// * or encrypted in conjunction with other encryption mechanisms.
#[derive(Clone, Serialize, Deserialize, SurrealValue)]
pub struct Jwt(pub(crate) String);

impl Jwt {
	/// Returns the underlying token string.
	///
	/// ⚠️: It is important to note that the token should be handled securely
	/// and protected from unauthorized access.
	pub fn as_insecure_token(&self) -> &str {
		&self.0
	}

	/// Returns the underlying token string.
	///
	/// ⚠️: It is important to note that the token should be handled securely
	/// and protected from unauthorized access.
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
