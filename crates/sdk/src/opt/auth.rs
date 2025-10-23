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
pub trait Credentials<Action>: SurrealValue {}

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

impl Credentials<Signin> for Root {}

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

impl Credentials<Signin> for Namespace {}

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

impl Credentials<Signin> for Database {}

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

impl<T, P> Credentials<T> for Record<P> where P: SurrealValue {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Token<T: SurrealValue = AccessToken> {
	pub access: T,
	pub refresh: Option<RefreshToken>,
}

impl<T: SurrealValue> SurrealValue for Token<T> {
	fn kind_of() -> Kind {
		kind!({ token: any, refresh: any })
	}

	fn into_value(self) -> Value {
		let mut obj = Object::new();
		obj.insert("token".to_string(), self.access.into_value());
		obj.insert("refresh".to_string(), self.refresh.into_value());
		Value::Object(obj)
	}

	fn from_value(value: Value) -> surrealdb_types::anyhow::Result<Self> {
		match value {
			value @ Value::String(_) => Ok(Token {
				access: T::from_value(value)?,
				refresh: None,
			}),
			value => {
				let mut obj = Object::from_value(value)?;
				let access = T::from_value(obj.remove("token").unwrap_or_default())?;
				let refresh = match obj.remove("refresh") {
					Some(value) => SurrealValue::from_value(value)?,
					None => None,
				};
				Ok(Token {
					access,
					refresh,
				})
			}
		}
	}
}

/// A JSON Web Token for authenticating with the server.
///
/// This struct represents a JSON Web Token (JWT) that can be used for
/// authentication purposes. It is important to note that this implementation
/// provide some security measures to protect the token:
/// * the debug implementation just prints `AccessToken(REDACTED)`,
/// * `Display` is not implemented so you can't call `.to_string()` on it
///
/// You can still have access to the token string using either
/// [`as_insecure_token`](AccessToken::as_insecure_token) or
/// [`into_insecure_token`](AccessToken::into_insecure_token) functions. However, you
/// should take care to ensure that only authorized users have access to the
/// JWT. For example, it can be stored in a secure cookie or encrypted in conjunction with other
/// encryption mechanisms.
#[derive(Clone, Serialize, Deserialize, SurrealValue)]
pub struct AccessToken(pub(crate) SecureToken);

impl AccessToken {
	/// Returns the underlying token string.
	///
	/// ⚠️: It is important to note that the token should be handled securely
	/// and protected from unauthorized access.
	pub fn as_insecure_token(&self) -> &str {
		&self.0.0
	}

	/// Returns the underlying token string.
	///
	/// ⚠️: It is important to note that the token should be handled securely
	/// and protected from unauthorized access.
	pub fn into_insecure_token(self) -> String {
		self.0.0
	}
}

impl fmt::Debug for AccessToken {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "AccessToken(REDACTED)")
	}
}

#[derive(Clone, Serialize, Deserialize, SurrealValue)]
pub struct RefreshToken(pub(crate) SecureToken);

impl RefreshToken {
	/// Returns the underlying token string.
	///
	/// ⚠️: It is important to note that the token should be handled securely
	/// and protected from unauthorized access.
	pub fn as_insecure_token(&self) -> &str {
		&self.0.0
	}

	/// Returns the underlying token string.
	///
	/// ⚠️: It is important to note that the token should be handled securely
	/// and protected from unauthorized access.
	pub fn into_insecure_token(self) -> String {
		self.0.0
	}
}

impl fmt::Debug for RefreshToken {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "RefreshToken(REDACTED)")
	}
}

#[derive(Clone, Serialize, Deserialize, SurrealValue)]
pub(crate) struct SecureToken(pub(crate) String);

impl From<AccessToken> for Token {
	fn from(access: AccessToken) -> Self {
		Self {
			access,
			refresh: None,
		}
	}
}

impl From<(AccessToken, RefreshToken)> for Token {
	fn from(token: (AccessToken, RefreshToken)) -> Self {
		Self {
			access: token.0,
			refresh: Some(token.1),
		}
	}
}

impl From<(Option<AccessToken>, Option<RefreshToken>)> for Token<Option<AccessToken>> {
	fn from(token: (Option<AccessToken>, Option<RefreshToken>)) -> Self {
		Self {
			access: token.0,
			refresh: token.1,
		}
	}
}

impl From<String> for Token {
	fn from(token: String) -> Self {
		Self {
			access: AccessToken(SecureToken(token)),
			refresh: None,
		}
	}
}

impl<'a> From<&'a String> for Token {
	fn from(token: &'a String) -> Self {
		Self {
			access: AccessToken(SecureToken(token.to_owned())),
			refresh: None,
		}
	}
}

impl<'a> From<&'a str> for Token {
	fn from(token: &'a str) -> Self {
		Self {
			access: AccessToken(SecureToken(token.to_owned())),
			refresh: None,
		}
	}
}

impl From<String> for AccessToken {
	fn from(token: String) -> Self {
		Self(SecureToken(token))
	}
}

impl<'a> From<&'a String> for AccessToken {
	fn from(token: &'a String) -> Self {
		Self(SecureToken(token.to_owned()))
	}
}

impl<'a> From<&'a str> for AccessToken {
	fn from(token: &'a str) -> Self {
		Self(SecureToken(token.to_owned()))
	}
}

impl From<String> for RefreshToken {
	fn from(token: String) -> Self {
		Self(SecureToken(token))
	}
}

impl<'a> From<&'a String> for RefreshToken {
	fn from(token: &'a String) -> Self {
		Self(SecureToken(token.to_owned()))
	}
}

impl<'a> From<&'a str> for RefreshToken {
	fn from(token: &'a str) -> Self {
		Self(SecureToken(token.to_owned()))
	}
}
