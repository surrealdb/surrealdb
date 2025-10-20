//! Authentication types

use std::fmt;

use serde::{Deserialize, Serialize};
use surrealdb_types::{Kind, Object, SurrealValue, Value, kind};

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
#[derive(Debug)]
pub struct Record<P: SurrealValue> {
	/// The namespace the user has access to
	pub namespace: String,
	/// The database the user has access to
	pub database: String,
	/// The access method to use for signin and signup
	pub access: String,
	/// The additional params to use
	pub params: P,
}

impl<P: SurrealValue> SurrealValue for Record<P> {
	fn kind_of() -> Kind {
		kind!({ ns: string, db: string, ac: string, params: any })
	}

	fn into_value(self) -> Value {
		let mut obj = Object::new();
		obj.insert("ns".to_string(), Value::String(self.namespace));
		obj.insert("db".to_string(), Value::String(self.database));
		obj.insert("ac".to_string(), Value::String(self.access));

		// Flatten the params into the top level object
		if let Value::Object(params_obj) = self.params.into_value() {
			for (key, value) in params_obj {
				obj.insert(key, value);
			}
		}

		Value::Object(obj)
	}

	fn from_value(value: Value) -> surrealdb_types::anyhow::Result<Self> {
		if let Value::Object(mut obj) = value {
			let namespace = obj
				.remove("ns")
				.and_then(|v| {
					if let Value::String(s) = v {
						Some(s)
					} else {
						None
					}
				})
				.ok_or_else(|| surrealdb_types::anyhow::anyhow!("Missing 'ns' field"))?;
			let database = obj
				.remove("db")
				.and_then(|v| {
					if let Value::String(s) = v {
						Some(s)
					} else {
						None
					}
				})
				.ok_or_else(|| surrealdb_types::anyhow::anyhow!("Missing 'db' field"))?;
			let access = obj
				.remove("ac")
				.and_then(|v| {
					if let Value::String(s) = v {
						Some(s)
					} else {
						None
					}
				})
				.ok_or_else(|| surrealdb_types::anyhow::anyhow!("Missing 'ac' field"))?;

			// The remaining fields go into params
			let params = P::from_value(Value::Object(obj))?;

			Ok(Record {
				namespace,
				database,
				access,
				params,
			})
		} else {
			Err(surrealdb_types::anyhow::anyhow!("Expected an object for Record"))
		}
	}
}

impl<T, P> Credentials<T, Jwt> for Record<P> where P: SurrealValue {}

#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub struct Jwt {
	pub access: Token,
	pub refresh: Option<Token>,
}

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
pub struct Token(pub(crate) String);

impl Token {
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

impl From<String> for Token {
	fn from(jwt: String) -> Self {
		Token(jwt)
	}
}

impl<'a> From<&'a String> for Token {
	fn from(jwt: &'a String) -> Self {
		Token(jwt.to_owned())
	}
}

impl<'a> From<&'a str> for Token {
	fn from(jwt: &'a str) -> Self {
		Token(jwt.to_owned())
	}
}

impl fmt::Debug for Token {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Token(REDACTED)")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn as_insecure_token() {
		let jwt = Token("super-long-jwt".to_owned());
		assert_eq!(jwt.as_insecure_token(), "super-long-jwt");
	}

	#[test]
	fn into_insecure_token() {
		let jwt = Token("super-long-jwt".to_owned());
		assert_eq!(jwt.into_insecure_token(), "super-long-jwt");
	}
}
