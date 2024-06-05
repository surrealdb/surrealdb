use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::DefineAccessStatement;
use crate::sql::{escape::quote_str, Algorithm};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

use super::{Object, Value};

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

impl AccessType {
	// Returns whether or not the access method can issue non-token grants
	// In this context, token refers exclusively to JWT
	#[allow(unreachable_patterns)]
	pub fn can_issue_grants(&self) -> bool {
		match self {
			// The grants for JWT and record access methods are JWT
			AccessType::Jwt(_) | AccessType::Record(_) => false,
			// TODO(gguillemas): This arm should be reachable by the bearer access method
			_ => unreachable!(),
		}
	}
	// Returns whether or not the access method can issue tokens
	// In this context, tokens refers exclusively to JWT
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

impl JwtAccess {
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

impl Display for AccessType {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			AccessType::Jwt(ac) => {
				f.write_str(" JWT")?;
				match &ac.verify {
					JwtAccessVerify::Key(ref v) => {
						write!(f, " ALGORITHM {} KEY {}", v.alg, quote_str(&v.key))?
					}
					JwtAccessVerify::Jwks(ref v) => write!(f, " JWKS {}", quote_str(&v.url))?,
				}
			}
			AccessType::Record(ac) => {
				f.write_str(" RECORD")?;
				if let Some(ref v) = ac.signup {
					write!(f, " SIGNUP {v}")?
				}
				if let Some(ref v) = ac.signin {
					write!(f, " SIGNIN {v}")?
				}
			}
		}
		Ok(())
	}
}

impl InfoStructure for AccessType {
	fn structure(self) -> Value {
		let mut acc = Object::default();

		match self {
			AccessType::Jwt(ac) => {
				acc.insert("kind".to_string(), "JWT".into());
				acc.insert("jwt".to_string(), ac.structure());
			}
			AccessType::Record(ac) => {
				acc.insert("kind".to_string(), "RECORD".into());
				if let Some(signup) = ac.signup {
					acc.insert("signup".to_string(), signup.structure());
				}
				if let Some(signin) = ac.signin {
					acc.insert("signin".to_string(), signin.structure());
				}
				acc.insert("jwt".to_string(), ac.jwt.structure());
			}
		};

		Value::Object(acc)
	}
}

impl Display for JwtAccess {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match &self.verify {
			JwtAccessVerify::Key(ref v) => {
				write!(f, "ALGORITHM {} KEY {}", v.alg, quote_str(&v.key))?;
			}
			JwtAccessVerify::Jwks(ref v) => {
				write!(f, "JWKS {}", quote_str(&v.url),)?;
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
		let mut acc = Object::default();
		match self.verify {
			JwtAccessVerify::Key(v) => {
				acc.insert("alg".to_string(), v.alg.structure());
				acc.insert("key".to_string(), v.key.into());
			}
			JwtAccessVerify::Jwks(v) => {
				acc.insert("jwks".to_string(), v.url.into());
			}
		}
		if let Some(v) = self.issue {
			let mut iss = Object::default();
			iss.insert("alg".to_string(), v.alg.structure());
			iss.insert("key".to_string(), v.key.into());
			acc.insert("issuer".to_string(), iss.to_string().into());
		}
		Value::Object(acc)
	}
}
