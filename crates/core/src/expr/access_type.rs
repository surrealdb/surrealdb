use std::fmt;
use std::fmt::Display;
use std::str::FromStr;

use anyhow::Result;
use revision::{Error as RevisionError, revisioned};
use serde::{Deserialize, Serialize};

use super::Value;
use crate::err::Error;
use crate::expr::escape::QuoteStr;
use crate::expr::statements::DefineAccessStatement;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Algorithm, Expr};

/// The type of access methods available
#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub enum AccessType {
	Record(RecordAccess),
	Jwt(JwtAccess),
	// TODO(gguillemas): Document once bearer access is no longer experimental.
	Bearer(BearerAccess),
}

// Allows retrieving the JWT configuration for any access type.
pub trait Jwt {
	fn jwt(&self) -> &JwtAccess;
}

impl Default for AccessType {
	fn default() -> Self {
		// Access type defaults to the most specific
		Self::Record(RecordAccess {
			..Default::default()
		})
	}
}

impl Jwt for AccessType {
	fn jwt(&self) -> &JwtAccess {
		match self {
			AccessType::Record(at) => at.jwt(),
			AccessType::Jwt(at) => at.jwt(),
			AccessType::Bearer(at) => at.jwt(),
		}
	}
}

impl Display for AccessType {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			AccessType::Jwt(ac) => {
				write!(f, "JWT {}", ac)?;
			}
			AccessType::Record(ac) => {
				f.write_str("RECORD")?;
				if let Some(ref v) = ac.signup {
					write!(f, " SIGNUP {v}")?
				}
				if let Some(ref v) = ac.signin {
					write!(f, " SIGNIN {v}")?
				}
				if ac.bearer.is_some() {
					write!(f, " WITH REFRESH")?
				}
				write!(f, " WITH JWT {}", ac.jwt)?;
			}
			AccessType::Bearer(ac) => {
				write!(f, "BEARER")?;
				match ac.subject {
					BearerAccessSubject::User => write!(f, " FOR USER")?,
					BearerAccessSubject::Record => write!(f, " FOR RECORD")?,
				}
			}
		}
		Ok(())
	}
}

impl InfoStructure for AccessType {
	fn structure(self) -> Value {
		match self {
			AccessType::Jwt(v) => Value::from(map! {
				"kind".to_string() => "JWT".into(),
				"jwt".to_string() => v.structure(),
			}),
			AccessType::Record(v) => Value::from(map! {
				"kind".to_string() => "RECORD".into(),
				"jwt".to_string() => v.jwt.structure(),
				"signup".to_string(), if let Some(v) = v.signup => v.structure(),
				"signin".to_string(), if let Some(v) = v.signin => v.structure(),
				"refresh".to_string(), if v.bearer.is_some() => true.into(),
			}),
			AccessType::Bearer(ac) => Value::from(map! {
					"kind".to_string() => "BEARER".into(),
					"subject".to_string() => match ac.subject {
							BearerAccessSubject::Record => "RECORD",
							BearerAccessSubject::User => "USER",
			}.into(),
					"jwt".to_string() => ac.jwt.structure(),
				}),
		}
	}
}

impl AccessType {
	/// Returns whether or not the access method can issue non-token grants
	/// In this context, token refers exclusively to JWT
	pub fn can_issue_grants(&self) -> bool {
		match self {
			// The JWT access method cannot issue stateful grants.
			AccessType::Jwt(_) => false,
			// The record access method can be used to issue grants if defined with bearer AKA
			// refresh.
			AccessType::Record(ac) => ac.bearer.is_some(),
			AccessType::Bearer(_) => true,
		}
	}
	/// Returns whether or not the access method can issue tokens
	/// In this context, tokens refers exclusively to JWT
	pub fn can_issue_tokens(&self) -> bool {
		match self {
			// The JWT access method can only issue tokens if an issuer is set
			AccessType::Jwt(jwt) => jwt.issue.is_some(),
			_ => true,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub struct JwtAccess {
	// Verify is required
	pub verify: JwtAccessVerify,
	// Issue is optional
	// It is possible to only verify externally issued tokens
	pub issue: Option<JwtAccessIssue>,
}

impl Default for JwtAccess {
	fn default() -> Self {
		// Defaults to HS512 with a randomly generated key
		let alg = Algorithm::Hs512;
		let key = DefineAccessStatement::random_key();
		// By default the access method can verify and issue tokens
		Self {
			verify: JwtAccessVerify::Key(JwtAccessVerifyKey {
				alg,
				key: key.clone(),
			}),
			issue: Some(JwtAccessIssue {
				alg,
				key,
			}),
		}
	}
}

impl Jwt for JwtAccess {
	fn jwt(&self) -> &JwtAccess {
		self
	}
}

impl Display for JwtAccess {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match &self.verify {
			JwtAccessVerify::Key(v) => {
				write!(f, "ALGORITHM {} KEY {}", v.alg, QuoteStr(&v.key))?;
			}
			JwtAccessVerify::Jwks(v) => {
				write!(f, "URL {}", QuoteStr(&v.url),)?;
			}
		}
		if let Some(iss) = &self.issue {
			write!(f, " WITH ISSUER KEY {}", QuoteStr(&iss.key))?;
		}
		Ok(())
	}
}

impl InfoStructure for JwtAccess {
	fn structure(self) -> Value {
		Value::from(map! {
			"verify".to_string() => match self.verify {
				JwtAccessVerify::Jwks(v) => Value::from(map!{
					"url".to_string() => v.url.into(),
				}),
				JwtAccessVerify::Key(v) => Value::from(map!{
					"alg".to_string() => v.alg.structure(),
					"key".to_string() => v.key.into(),
				}),
			},
			"issuer".to_string(), if let Some(v) = self.issue => Value::from(map!{
				"alg".to_string() => v.alg.structure(),
				"key".to_string() => v.key.into(),
			}),
		})
	}
}

impl JwtAccess {
	/// Redacts certain parts of the definition for security on export.
	pub(crate) fn redacted(&self) -> JwtAccess {
		let mut jwt = self.clone();
		jwt.verify = match jwt.verify {
			JwtAccessVerify::Key(mut key) => {
				// If algorithm is symmetric, the verification key is a secret
				if key.alg.is_symmetric() {
					key.key = "[REDACTED]".to_string();
				}
				JwtAccessVerify::Key(key)
			}
			// No secrets in JWK
			JwtAccessVerify::Jwks(jwks) => JwtAccessVerify::Jwks(jwks),
		};
		jwt.issue = match jwt.issue {
			Some(mut issue) => {
				issue.key = "[REDACTED]".to_string();
				Some(issue)
			}
			None => None,
		};
		jwt
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub struct JwtAccessIssue {
	pub alg: Algorithm,
	pub key: String,
}

impl Default for JwtAccessIssue {
	fn default() -> Self {
		Self {
			// Defaults to HS512
			alg: Algorithm::Hs512,
			// Avoid defaulting to empty key
			key: DefineAccessStatement::random_key(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub enum JwtAccessVerify {
	Key(JwtAccessVerifyKey),
	Jwks(JwtAccessVerifyJwks),
}

impl Default for JwtAccessVerify {
	fn default() -> Self {
		Self::Key(JwtAccessVerifyKey {
			..Default::default()
		})
	}
}

impl InfoStructure for JwtAccessVerify {
	fn structure(self) -> Value {
		match self {
			JwtAccessVerify::Jwks(v) => Value::from(map! {
				"url".to_string() => v.url.into(),
			}),
			JwtAccessVerify::Key(v) => Value::from(map! {
				"alg".to_string() => v.alg.structure(),
				"key".to_string() => v.key.into(),
			}),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub struct JwtAccessVerifyKey {
	pub alg: Algorithm,
	pub key: String,
}

impl Default for JwtAccessVerifyKey {
	fn default() -> Self {
		Self {
			// Defaults to HS512
			alg: Algorithm::Hs512,
			// Avoid defaulting to empty key
			key: DefineAccessStatement::random_key(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub struct JwtAccessVerifyJwks {
	pub url: String,
}

#[revisioned(revision = 4)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub struct RecordAccess {
	pub signup: Option<Expr>,
	pub signin: Option<Expr>,
	pub jwt: JwtAccess,
	#[revision(start = 2, end = 3, convert_fn = "authenticate_revision")]
	pub authenticate: Option<Value>,
	#[revision(start = 4)]
	pub bearer: Option<BearerAccess>,
}

impl RecordAccess {
	fn authenticate_revision(
		&self,
		_revision: u16,
		_value: Option<Value>,
	) -> Result<(), RevisionError> {
		Err(RevisionError::Conversion(
			"The \"AUTHENTICATE\" clause has been moved to \"DEFINE ACCESS\"".to_string(),
		))
	}
}

impl Default for RecordAccess {
	fn default() -> Self {
		Self {
			signup: None,
			signin: None,
			jwt: JwtAccess {
				..Default::default()
			},
			bearer: None,
		}
	}
}

impl Jwt for RecordAccess {
	fn jwt(&self) -> &JwtAccess {
		&self.jwt
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub struct BearerAccess {
	pub kind: BearerAccessType,
	pub subject: BearerAccessSubject,
	pub jwt: JwtAccess,
}

impl Default for BearerAccess {
	fn default() -> Self {
		Self {
			kind: BearerAccessType::Bearer,
			subject: BearerAccessSubject::User,
			jwt: JwtAccess {
				..Default::default()
			},
		}
	}
}

impl Jwt for BearerAccess {
	fn jwt(&self) -> &JwtAccess {
		&self.jwt
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub enum BearerAccessType {
	Bearer,
	Refresh,
}

impl BearerAccessType {
	pub fn prefix(&self) -> &'static str {
		match self {
			Self::Bearer => "surreal-bearer",
			Self::Refresh => "surreal-refresh",
		}
	}
}

impl FromStr for BearerAccessType {
	type Err = Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s.to_ascii_lowercase().as_str() {
			"bearer" => Ok(Self::Bearer),
			"refresh" => Ok(Self::Refresh),
			_ => Err(Error::AccessGrantBearerInvalid),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub enum BearerAccessSubject {
	Record,
	User,
}
