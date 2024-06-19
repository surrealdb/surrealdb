use super::Value;
use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::DefineAccessStatement;
use crate::sql::{escape::quote_str, Algorithm};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

/// The type of access methods available
#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum AccessType {
	Record(RecordAccess),
	Jwt(JwtAccess),
}

impl Default for AccessType {
	fn default() -> Self {
		// Access type defaults to the most specific
		Self::Record(RecordAccess {
			..Default::default()
		})
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
				write!(f, " WITH JWT {}", ac.jwt)?;
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
			}),
		}
	}
}

impl AccessType {
	/// Returns whether or not the access method can issue non-token grants
	/// In this context, token refers exclusively to JWT
	#[allow(unreachable_patterns)]
	pub fn can_issue_grants(&self) -> bool {
		match self {
			// The grants for JWT and record access methods are JWT
			AccessType::Jwt(_) | AccessType::Record(_) => false,
			// TODO(gguillemas): This arm should be reachable by the bearer access method
			_ => unreachable!(),
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
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl Display for JwtAccess {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match &self.verify {
			JwtAccessVerify::Key(ref v) => {
				write!(f, "ALGORITHM {} KEY {}", v.alg, quote_str(&v.key))?;
			}
			JwtAccessVerify::Jwks(ref v) => {
				write!(f, "URL {}", quote_str(&v.url),)?;
			}
		}
		if let Some(iss) = &self.issue {
			write!(f, " WITH ISSUER KEY {}", quote_str(&iss.key))?;
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
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct JwtAccessVerifyJwks {
	pub url: String,
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordAccess {
	pub signup: Option<Value>,
	pub signin: Option<Value>,
	pub jwt: JwtAccess,
}

impl Default for RecordAccess {
	fn default() -> Self {
		Self {
			signup: None,
			signin: None,
			jwt: JwtAccess {
				..Default::default()
			},
		}
	}
}
