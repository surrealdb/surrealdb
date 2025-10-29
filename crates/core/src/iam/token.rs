use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;

use anyhow::{Result, bail};
use jsonwebtoken::{Algorithm, Header, decode};
use serde::{Deserialize, Serialize};
use surrealdb_types::SurrealValue;

use crate::dbs::Session;
use crate::err::Error;
use crate::iam::verify::{DUD, KEY};
use crate::kvs::Datastore;
use crate::sql::expression::convert_public_value_to_internal;
use crate::val::{Object, Value, convert_object_to_public_map};
use crate::{iam, syn};
pub static HEADER: LazyLock<Header> = LazyLock::new(|| Header::new(Algorithm::HS512));

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
/// use surrealdb_core::iam::token::Token;
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

impl Token {
	/// Refreshes an access token using a refresh token.
	///
	/// This method exchanges an expired (or soon-to-expire) access token for a new one
	/// using the provided refresh token. The refresh process follows OAuth2/JWT best practices
	/// by maintaining the original authentication scope from the access token claims.
	///
	/// # Authentication Scope vs Working Context
	///
	/// It's important to understand the distinction between authentication scope and working
	/// context:
	///
	/// - **Authentication Scope** (from token claims): The namespace, database, and access method
	///   that were used during the original signin. This represents *what you're authenticated as*.
	///
	/// - **Working Context** (from session fields): The current namespace and database set by the
	///   `USE` command. This represents *where you're currently working*.
	///
	/// During refresh, the authentication scope from the expired access token is used to create
	/// the new token, and the session is restored to match this original scope. This means:
	///
	/// 1. If you signin to `ns1/db1`, then call `USE ns2 db2`, then refresh:
	///    - The session will be restored to `ns1/db1` (original authentication scope)
	///    - You can call `USE ns2 db2` again after refresh if needed
	///
	/// 2. The refresh token is validated against the namespace/database from the original signin,
	///    not the current session working context.
	///
	/// This behavior is intentional and follows security best practices:
	/// - Prevents scope confusion or escalation
	/// - Maintains predictable authentication boundaries
	/// - Aligns with OAuth2/OIDC refresh token standards
	///
	/// # Arguments
	///
	/// * `kvs` - The datastore to validate the refresh token against
	/// * `session` - The session to update with the new authentication state
	///
	/// # Returns
	///
	/// Returns a new `Token` with fresh access and refresh tokens on success.
	///
	/// # Errors
	///
	/// Returns an error if:
	/// - The token is an `Access` variant without a refresh token
	/// - The refresh token is invalid, expired, or revoked
	/// - The access token cannot be decoded
	/// - The signin process fails
	///
	/// # Example
	///
	/// ```ignore
	/// // Signin and get tokens
	/// let token = iam::signin::signin(kvs, session, credentials).await?;
	///
	/// // Later, when the access token expires...
	/// let new_token = token.refresh(kvs, session).await?;
	/// ```
	pub async fn refresh(self, kvs: &Datastore, session: &mut Session) -> Result<Self> {
		match self {
			Token::Access(_) => bail!(Error::InvalidArguments {
				name: "refresh".into(),
				message: "Token is an access token, cannot refresh".into(),
			}),
			Token::WithRefresh {
				access,
				refresh,
			} => {
				// Decode the expired access token to extract its claims.
				// We don't verify the signature or expiration here because we're only
				// extracting the authentication scope (NS, DB, AC, ID, etc.) to pass
				// to the signin function. The refresh token itself will be validated
				// during the signin process.
				let token_data = decode::<Claims>(&access, &KEY, &DUD)?;
				let claims = token_data.claims.into_claims_object();
				// Convert token claims to signin variables. These claims contain the
				// original authentication scope (namespace, database, access method)
				// that will be used to create the new tokens.
				let mut vars = convert_object_to_public_map(claims)?;
				// Add the refresh token to the variables. The signin function will
				// use this to perform bearer authentication and validate the refresh token.
				vars.insert("refresh".to_string(), refresh.into_value());
				// Perform signin using the refresh token. This will:
				// 1. Validate the refresh token against the stored grant
				// 2. Revoke the old refresh token (single-use)
				// 3. Create a new access token and refresh token
				// 4. Update the session with the original authentication scope
				iam::signin::signin(kvs, session, vars.into()).await
			}
		}
	}

	pub async fn revoke_refresh_token(self, kvs: &Datastore) -> Result<()> {
		match self {
			Token::Access(_) => bail!(Error::InvalidArguments {
				name: "refresh".into(),
				message: "Token is an access token, cannot revoke refresh token".into(),
			}),
			Token::WithRefresh {
				access,
				refresh,
			} => {
				let grant_id = iam::signin::validate_grant_bearer(&refresh)?;
				let token_data = decode::<Claims>(&access, &KEY, &DUD)?;
				let ns = token_data.claims.ns.ok_or_else(|| Error::InvalidArguments {
					name: "ns".into(),
					message: "Token does not contain a namespace".into(),
				})?;
				let db = token_data.claims.db.ok_or_else(|| Error::InvalidArguments {
					name: "db".into(),
					message: "Token does not contain a database".into(),
				})?;
				let ac = token_data.claims.ac.ok_or_else(|| Error::InvalidArguments {
					name: "ac".into(),
					message: "Token does not contain an access name".into(),
				})?;
				iam::access::revoke_refresh_token_record(kvs, grant_id, ac, &ns, &db).await?;
				Ok(())
			}
		}
	}
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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Audience {
	Single(String),
	Multiple(Vec<String>),
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Claims {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub iat: Option<i64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub nbf: Option<i64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub exp: Option<i64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub iss: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub sub: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub aud: Option<Audience>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub jti: Option<String>,
	#[serde(alias = "ns")]
	#[serde(alias = "NS")]
	#[serde(rename = "NS")]
	#[serde(alias = "https://surrealdb.com/ns")]
	#[serde(alias = "https://surrealdb.com/namespace")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ns: Option<String>,
	#[serde(alias = "db")]
	#[serde(alias = "DB")]
	#[serde(rename = "DB")]
	#[serde(alias = "https://surrealdb.com/db")]
	#[serde(alias = "https://surrealdb.com/database")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub db: Option<String>,
	#[serde(alias = "ac")]
	#[serde(alias = "AC")]
	#[serde(rename = "AC")]
	#[serde(alias = "https://surrealdb.com/ac")]
	#[serde(alias = "https://surrealdb.com/access")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub ac: Option<String>,
	#[serde(alias = "id")]
	#[serde(alias = "ID")]
	#[serde(rename = "ID")]
	#[serde(alias = "https://surrealdb.com/id")]
	#[serde(alias = "https://surrealdb.com/record")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub id: Option<String>,
	#[serde(alias = "rl")]
	#[serde(alias = "RL")]
	#[serde(rename = "RL")]
	#[serde(alias = "https://surrealdb.com/rl")]
	#[serde(alias = "https://surrealdb.com/roles")]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub roles: Option<Vec<String>>,

	#[serde(flatten)]
	#[serde(skip_serializing_if = "Option::is_none")]
	pub custom_claims: Option<HashMap<String, serde_json::Value>>,
}

impl Claims {
	pub(crate) fn into_claims_object(self) -> Object {
		// Set default value
		let mut out = Object::default();
		// Add iss field if set
		if let Some(iss) = self.iss {
			out.insert("iss".to_string(), iss.into());
		}
		// Add sub field if set
		if let Some(sub) = self.sub {
			out.insert("sub".to_string(), sub.into());
		}
		// Add aud field if set
		if let Some(aud) = self.aud {
			match aud {
				Audience::Single(v) => out.insert("aud".to_string(), Value::String(v)),
				Audience::Multiple(v) => out.insert(
					"aud".to_string(),
					v.into_iter().map(Value::String).collect::<Vec<_>>().into(),
				),
			};
		}
		// Add iat field if set
		if let Some(iat) = self.iat {
			out.insert("iat".to_string(), iat.into());
		}
		// Add nbf field if set
		if let Some(nbf) = self.nbf {
			out.insert("nbf".to_string(), nbf.into());
		}
		// Add exp field if set
		if let Some(exp) = self.exp {
			out.insert("exp".to_string(), exp.into());
		}
		// Add jti field if set
		if let Some(jti) = self.jti {
			out.insert("jti".to_string(), jti.into());
		}
		// Add NS field if set
		if let Some(ns) = self.ns {
			out.insert("NS".to_string(), ns.into());
		}
		// Add DB field if set
		if let Some(db) = self.db {
			out.insert("DB".to_string(), db.into());
		}
		// Add AC field if set
		if let Some(ac) = self.ac {
			out.insert("AC".to_string(), ac.into());
		}
		// Add ID field if set
		if let Some(id) = self.id {
			out.insert("ID".to_string(), id.into());
		}
		// Add RL field if set
		if let Some(role) = self.roles {
			out.insert(
				"RL".to_string(),
				role.into_iter().map(Value::from).collect::<Vec<_>>().into(),
			);
		}
		// Add custom claims if set
		if let Some(custom_claims) = self.custom_claims {
			for (claim, value) in custom_claims {
				// Serialize the raw JSON string representing the claim value
				let claim_json = match serde_json::to_string(&value) {
					Ok(claim_json) => claim_json,
					Err(err) => {
						debug!("Failed to serialize token claim '{}': {}", claim, err);
						continue;
					}
				};
				// Parse that JSON string into the corresponding SurrealQL value
				let claim_value = match syn::json(&claim_json) {
					Ok(claim_value) => claim_value,
					Err(err) => {
						debug!("Failed to parse token claim '{}': {}", claim, err);
						continue;
					}
				};
				let claim_value = convert_public_value_to_internal(claim_value);
				out.insert(claim.clone(), claim_value);
			}
		}
		// Return value
		out
	}
}
