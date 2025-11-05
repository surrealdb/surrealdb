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

/// A token containing both access and optional refresh token for authentication.
///
/// This struct represents the complete authentication token response from
/// SurrealDB's signin and signup operations. It contains an access token
/// (required) and an optional refresh token for enhanced security.
///
/// The token structure supports both legacy single-token authentication
/// and modern refresh token flows:
/// - **Legacy mode**: Only `access` token is present, `refresh` is `None`
/// - **Refresh mode**: Both `access` and `refresh` tokens are present
///
/// # Security
///
/// Both access and refresh tokens are wrapped in secure containers that:
/// - Redact token values in debug output
/// - Prevent accidental exposure in logs
/// - Require explicit methods to access token strings
///
/// # Examples
///
/// ```rust
/// use surrealdb::opt::auth::{Token, AccessToken, RefreshToken};
///
/// // Create a token with only access token (legacy mode)
/// let legacy_token = Token {
///     access: AccessToken::from("access_token_string"),
///     refresh: None,
/// };
///
/// // Create a token with both access and refresh tokens
/// let modern_token = Token {
///     access: AccessToken::from("access_token_string"),
///     refresh: Some(RefreshToken::from("refresh_token_string")),
/// };
///
/// // Access token values securely
/// let access_value = modern_token.access.as_insecure_token();
/// if let Some(refresh_token) = &modern_token.refresh {
///     let refresh_value = refresh_token.as_insecure_token();
///     // Use refresh token to get new access token
/// }
/// ```
#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Token {
	/// The access token used for API authentication.
	///
	/// This token is required and used to authenticate API requests.
	/// It typically has a shorter expiration time for security.
	pub access: AccessToken,
	/// An optional refresh token used to obtain new access tokens.
	///
	/// When present, this token can be used to refresh the access token
	/// without requiring the user to re-authenticate. This enables
	/// seamless long-term sessions while maintaining security.
	pub refresh: Option<RefreshToken>,
}

impl SurrealValue for Token {
	fn kind_of() -> Kind {
		kind!(string | { access: string, refresh: string })
	}

	fn into_value(self) -> Value {
		match self.refresh {
			Some(refresh) => {
				let mut obj = Object::new();
				obj.insert("access".to_string(), self.access.into_value());
				obj.insert("refresh".to_string(), refresh.into_value());
				Value::Object(obj)
			}
			None => self.access.into_value(),
		}
	}

	fn from_value(value: Value) -> surrealdb_types::anyhow::Result<Self> {
		match value {
			Value::String(string) => Ok(Token::from(string)),
			value => {
				let mut obj = Object::from_value(value)?;
				let access = AccessToken::from_value(obj.remove("access").unwrap_or_default())?;
				let refresh = RefreshToken::from_value(obj.remove("refresh").unwrap_or_default())?;
				Ok(Token::from((access, refresh)))
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
#[derive(Debug, Serialize, Deserialize, SurrealValue)]
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

/// A refresh token used to obtain new access tokens without re-authentication.
///
/// Refresh tokens are long-lived tokens that can be used to obtain new access
/// tokens when the current access token expires. This enables seamless user
/// sessions while maintaining security through short-lived access tokens.
///
/// # Security Features
///
/// - **Debug Protection**: Token values are redacted in debug output
/// - **Secure Storage**: Wrapped in a secure container to prevent accidental exposure
/// - **Explicit Access**: Requires explicit method calls to access the token string
///
/// # Usage
///
/// Refresh tokens are typically used in the following flow:
/// 1. User authenticates and receives both access and refresh tokens
/// 2. Access token is used for API requests
/// 3. When access token expires, refresh token is used to get a new access token
/// 4. Process repeats until refresh token expires or user logs out
///
/// # Examples
///
/// ```rust
/// use surrealdb::opt::auth::RefreshToken;
///
/// // Create a refresh token
/// let refresh_token = RefreshToken::from("refresh_token_string");
///
/// // Access the token string securely
/// let token_string = refresh_token.as_insecure_token();
///
/// // Use the token string to request a new access token
/// // (implementation depends on your authentication flow)
/// ```
#[derive(Debug, Serialize, Deserialize, SurrealValue)]
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

/// A secure wrapper for token strings that prevents accidental exposure.
///
/// This internal struct wraps token strings to provide security features:
/// - Prevents accidental exposure in debug output
/// - Requires explicit method calls to access the underlying string
/// - Provides a clear API for secure token handling
///
/// The struct is marked as `pub(crate)` to keep it internal to the crate
/// while still allowing access from other modules within the same crate.
#[derive(Clone, Serialize, Deserialize, SurrealValue)]
pub(crate) struct SecureToken(pub(crate) String);

impl fmt::Debug for SecureToken {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "REDACTED")
	}
}

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

impl From<(AccessToken, Option<RefreshToken>)> for Token {
	fn from(token: (AccessToken, Option<RefreshToken>)) -> Self {
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

impl From<RefreshToken> for SecureToken {
	fn from(token: RefreshToken) -> Self {
		token.0
	}
}
