//! Authentication types

use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use thiserror::Error;

/// A signup action
#[derive(Debug)]
pub struct Signup;

/// A signin action
#[derive(Debug)]
pub struct Signin;

/// Credentials level
#[derive(Debug, Clone)]
pub enum CredentialsLevel {
	Root,
	Namespace,
	Database,
	Scope,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
	#[error("Username is needed for authentication but it was not provided")]
	NoUsername,
	#[error("Password is needed for authentication but it was not provided")]
	NoPassword,
	#[error("Namespace is needed for authentication but it was not provided")]
	NoNamespace,
	#[error("Database is needed for authentication but it was not provided")]
	NoDatabase,
	#[error("Scope is needed for authentication but it was not provided")]
	NoScope,
	#[error("Scope params are needed for authentication but they were not provided")]
	NoScopeParams,
}

/// Construct a Credentials instance for the given auth level
#[derive(Debug, Default)]
pub struct CredentialsBuilder<'a> {
	/// The auth username
	pub username: Option<&'a str>,
	/// The auth password
	pub password: Option<&'a str>,
	/// The auth namespace
	pub namespace: Option<&'a str>,
	/// The auth database
	pub database: Option<&'a str>,
	/// The auth scope
	pub scope: Option<&'a str>,
}

impl<'a> CredentialsBuilder<'a> {
	// Builder methods
	pub fn with_username(mut self, username: Option<&'a str>) -> Self {
		self.username = username;
		self
	}

	pub fn with_password(mut self, password: Option<&'a str>) -> Self {
		self.password = password;
		self
	}

	pub fn with_namespace(mut self, namespace: Option<&'a str>) -> Self {
		self.namespace = namespace;
		self
	}

	pub fn with_database(mut self, database: Option<&'a str>) -> Self {
		self.database = database;
		self
	}

	pub fn with_scope(mut self, scope: Option<&'a str>) -> Self {
		self.scope = scope;
		self
	}

	pub fn for_root(self) -> Result<Root<'a>, Error> {
		Ok(Root {
			username: self.username.ok_or(Error::NoUsername)?,
			password: self.password.ok_or(Error::NoPassword)?,
		})
	}

	pub fn for_namespace(self) -> Result<Namespace<'a>, Error> {
		Ok(Namespace {
			username: self.username.ok_or(Error::NoUsername)?,
			password: self.password.ok_or(Error::NoPassword)?,
			namespace: self.namespace.ok_or(Error::NoNamespace)?,
		})
	}

	pub fn for_database(self) -> Result<Database<'a>, Error> {
		Ok(Database {
			username: self.username.ok_or(Error::NoUsername)?,
			password: self.password.ok_or(Error::NoPassword)?,
			namespace: self.namespace.ok_or(Error::NoNamespace)?,
			database: self.database.ok_or(Error::NoDatabase)?,
		})
	}

	pub fn for_scope<P>(self, params: P) -> Result<Scope<'a, P>, Error> {
		Ok(Scope {
			namespace: self.namespace.ok_or(Error::NoNamespace)?,
			database: self.database.ok_or(Error::NoDatabase)?,
			scope: self.scope.ok_or(Error::NoScope)?,
			params,
		})
	}
}

/// Credentials for authenticating with the server
pub trait Credentials<Action, Response>: Serialize {}

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

impl Credentials<Signin, Jwt> for Root<'_> {}

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

impl Credentials<Signin, Jwt> for Namespace<'_> {}

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

impl Credentials<Signin, Jwt> for Database<'_> {}

/// Credentials for the scope user
#[derive(Debug, Serialize)]
pub struct Scope<'a, P> {
	/// The namespace the user has access to
	#[serde(rename = "ns")]
	pub namespace: &'a str,
	/// The database the user has access to
	#[serde(rename = "db")]
	pub database: &'a str,
	/// The scope to use for signin and signup
	#[serde(rename = "sc")]
	pub scope: &'a str,
	/// The additional params to use
	#[serde(flatten)]
	pub params: P,
}

impl<T, P> Credentials<T, Jwt> for Scope<'_, P> where P: Serialize {}

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
