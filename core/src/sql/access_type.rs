use crate::sql::statements::info::InfoStructure;
use crate::sql::{escape::quote_str, Algorithm, Duration};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

use super::{Access, Object, Value};

/// The type of access methods available
#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum AccessType {
	Jwt(JwtAccess),
	Record(RecordAccess),
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
pub struct JwtAccess {
	pub verification: JwtAccessVerification,
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
pub enum JwtAccessVerification {
	Key(JwtAccessVerificationKey),
	Jwks(JwtAccessVerificationJwks),
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
pub struct JwtAccessVerificationKey {
	pub alg: Algorithm,
	pub key: String,
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
pub struct JwtAccessVerificationJwks {
	pub url: String,
}

#[revisioned(revision = 1)]
#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq, PartialOrd)]
pub struct RecordAccess {
	pub duration: Option<Duration>,
	pub signup: Option<Value>,
	pub signin: Option<Value>,
}

impl Default for AccessType {
	fn default() -> Self {
		Self::Record(RecordAccess {
			duration: None,
			signup: None,
			signin: None,
		})
	}
}

impl Display for AccessType {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			AccessType::Jwt(ac) => {
				f.write_str(" JWT")?;
				match &ac.verification {
					JwtAccessVerification::Key(ref v) => {
						write!(f, " ALGORITHM {} KEY {}", v.alg, quote_str(&v.key))?
					}
					JwtAccessVerification::Jwks(ref v) => write!(f, " JWKS {}", quote_str(&v.url))?,
				}
			}
			AccessType::Record(ac) => {
				f.write_str(" RECORD")?;
				if let Some(ref v) = ac.duration {
					write!(f, " DURATION {v}")?
				}
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
				match ac.verification {
					JwtAccessVerification::Key(v) => {
						acc.insert("alg".to_string(), v.alg.structure());
						acc.insert("key".to_string(), v.key.into());
					}
					JwtAccessVerification::Jwks(v) => {
						acc.insert("url".to_string(), v.url.into());
					}
				}
			}
			AccessType::Record(ac) => {
				acc.insert("kind".to_string(), "RECORD".into());
				if let Some(signup) = ac.signup {
					acc.insert("signup".to_string(), signup.structure());
				}
				if let Some(signin) = ac.signin {
					acc.insert("signin".to_string(), signin.structure());
				}
				if let Some(duration) = ac.duration {
					acc.insert("duration".to_string(), duration.into());
				}
			}
		};

		Value::Object(acc)
	}
}

fn get_accesses_from_kind(accesses: &[Access]) -> Vec<&str> {
	accesses.iter().map(|t| t.0.as_str()).collect::<Vec<_>>()
}
