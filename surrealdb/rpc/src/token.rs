use std::fmt;

use surrealdb_types::SurrealValue;

/// A token that can be either an access token alone or an access token with a refresh token.
///
/// This enum supports two authentication scenarios:
/// - **Access-only**: A single access token for basic authentication
/// - **With refresh**: An access token paired with a refresh token for enhanced security
///
/// The enum uses untagged serialization, meaning it will serialize as either:
/// - A string (for access-only tokens)
/// - An object with `access` and `refresh` fields (for tokens with refresh)
///
/// # Refresh Token Flow
///
/// When using the `WithRefresh` variant, the token can be refreshed to obtain a new access token
/// without requiring the user to re-authenticate. The refresh process:
///
/// 1. Extracts the authentication scope (namespace, database, access method) from the expired
///    access token's JWT claims
/// 2. Uses the refresh token to authenticate and validate the request
/// 3. Revokes the old refresh token (refresh tokens are single-use)
/// 4. Issues a new access token and refresh token pair
/// 5. Restores the session to the original authentication scope
///
/// This ensures that refresh maintains the original authentication boundaries and prevents
/// scope confusion or escalation.
///
/// # Examples
///
/// ```rust
/// use surrealdb_rpc::Token;
///
/// // Access-only token
/// let access_token = Token::Access("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...".to_string());
///
/// // Token with refresh capability
/// let token_with_refresh = Token::WithRefresh {
///     access: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...".to_string(),
///     refresh: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...".to_string(),
/// };
/// ```
#[derive(Clone, Eq, PartialEq, PartialOrd, SurrealValue, Hash)]
#[surreal(crate = "surrealdb_types")]
#[surreal(untagged)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Token {
	/// An access token without a refresh token.
	///
	/// This variant represents the traditional authentication model where
	/// only a single access token is provided.
	Access(String),
	/// An access token paired with a refresh token.
	///
	/// This variant enables the refresh token flow, allowing clients to
	/// obtain new access tokens without re-authenticating when the access
	/// token expires.
	WithRefresh {
		/// The access token used for API authentication
		access: String,
		/// The refresh token used to obtain new access tokens
		refresh: String,
	},
}

impl fmt::Debug for Token {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Token::Access(_) => write!(f, "Token::Access(REDACTED)"),
			Token::WithRefresh {
				..
			} => write!(f, "Token::WithRefresh {{ access: REDACTED, refresh: REDACTED }}"),
		}
	}
}
