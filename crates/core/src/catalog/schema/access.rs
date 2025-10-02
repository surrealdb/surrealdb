use std::fmt;
use std::time::Duration;

use revision::revisioned;
use surrealdb_types::sql::ToSql;

use crate::expr::Expr;
use crate::expr::statements::info::InfoStructure;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::Value;

/// The type of access methods available
#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum AccessType {
	Record(RecordAccess),
	Jwt(JwtAccess),
	// TODO(gguillemas): Document once bearer access is no longer experimental.
	Bearer(BearerAccess),
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

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub(crate) struct RecordAccess {
	pub signup: Option<Expr>,
	pub signin: Option<Expr>,
	pub jwt: JwtAccess,
	pub bearer: Option<BearerAccess>,
}

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct BearerAccess {
	pub kind: BearerAccessType,
	pub subject: BearerAccessSubject,
	pub jwt: JwtAccess,
}

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Copy, Eq, PartialEq)]
pub enum BearerAccessType {
	Bearer,
	Refresh,
}

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Copy, Eq, PartialEq)]
pub enum BearerAccessSubject {
	Record,
	User,
}

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct JwtAccess {
	// Verify is required
	pub verify: JwtAccessVerify,
	// Issue is optional
	// It is possible to only verify externally issued tokens
	pub issue: Option<JwtAccessIssue>,
}

impl InfoStructure for JwtAccess {
	fn structure(self) -> Value {
		Value::from(map! {
			"verify".to_string() => match self.verify {
				JwtAccessVerify::Jwks(v) => Value::from(map!{
					"url".to_string() => v.url.into(),
				}),
				JwtAccessVerify::Key(v) => {
					if v.alg.is_symmetric(){
						Value::from(map!{
							"alg".to_string() => v.alg.to_sql().unwrap().into(),
							"key".to_string() => "[REDACTED]".to_string().into(),
						})
					}else{
						Value::from(map!{
							"alg".to_string() => v.alg.to_sql().unwrap().into(),
							"key".to_string() => v.key.into(),
						})
					}
				},
			},
			"issuer".to_string(), if let Some(v) = self.issue => Value::from(map!{
				"alg".to_string() => v.alg.to_sql().unwrap().into(),
				"key".to_string() => "[REDACTED]".to_string().into(),
			}),
		})
	}
}

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub enum JwtAccessVerify {
	Key(JwtAccessVerifyKey),
	Jwks(JwtAccessVerifyJwks),
}

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct JwtAccessVerifyKey {
	pub alg: Algorithm,
	pub key: String,
}

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct JwtAccessVerifyJwks {
	pub url: String,
}

#[revisioned(revision = 1)]
#[derive(Debug, Hash, Clone, Eq, PartialEq)]
pub struct JwtAccessIssue {
	pub alg: Algorithm,
	pub key: String,
}

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Algorithm {
	EdDSA,
	Es256,
	Es384,
	Es512,
	Hs256,
	Hs384,
	Hs512,
	Ps256,
	Ps384,
	Ps512,
	Rs256,
	Rs384,
	Rs512,
}

impl Algorithm {
	// Does the algorithm use the same key for signing and verification?
	pub(crate) fn is_symmetric(self) -> bool {
		matches!(self, Algorithm::Hs256 | Algorithm::Hs384 | Algorithm::Hs512)
	}
}

impl fmt::Display for Algorithm {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str(match self {
			Self::EdDSA => "EDDSA",
			Self::Es256 => "ES256",
			Self::Es384 => "ES384",
			Self::Es512 => "ES512",
			Self::Hs256 => "HS256",
			Self::Hs384 => "HS384",
			Self::Hs512 => "HS512",
			Self::Ps256 => "PS256",
			Self::Ps384 => "PS384",
			Self::Ps512 => "PS512",
			Self::Rs256 => "RS256",
			Self::Rs384 => "RS384",
			Self::Rs512 => "RS512",
		})
	}
}

impl ToSql for Algorithm {
	fn fmt_sql(&self, f: &mut String) -> std::fmt::Result {
		self.to_string().fmt_sql(f)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct AccessDefinition {
	pub(crate) name: String,
	pub(crate) access_type: AccessType,
	pub(crate) authenticate: Option<Expr>,
	pub(crate) grant_duration: Option<Duration>,
	pub(crate) token_duration: Option<Duration>,
	pub(crate) session_duration: Option<Duration>,
	pub(crate) comment: Option<String>,
}
impl_kv_value_revisioned!(AccessDefinition);

impl InfoStructure for AccessDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => Value::from(self.name.clone()),
			"authenticate".to_string(), if let Some(v) = self.authenticate => v.structure(),
			"duration".to_string() => Value::from(map!{
				"session".to_string() => self.session_duration.map(Value::from).unwrap_or(Value::None),
				"grant".to_string(), if self.access_type.can_issue_grants() => self.grant_duration.map(Value::from).unwrap_or(Value::None),
				"token".to_string(), if self.access_type.can_issue_tokens() => self.token_duration.map(Value::from).unwrap_or(Value::None),
			}),
			"kind".to_string() => self.access_type.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
