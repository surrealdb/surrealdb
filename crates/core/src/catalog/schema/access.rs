use std::fmt;
use std::time::Duration;

use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::catalog::schema::base::Base;
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
					BearerAccessSubject::Record => "RECORD".into(),
					BearerAccessSubject::User => "USER".into(),
				},
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
					"url".to_string() => v.url.clone().into(),
				}),
				JwtAccessVerify::Key(v) => {
					if v.alg.is_symmetric(){
						Value::from(map!{
							"alg".to_string() => v.alg.to_string().into(),
							"key".to_string() => "[REDACTED]".into(),
						})
					}else{
						Value::from(map!{
							"alg".to_string() => v.alg.to_string().into(),
							"key".to_string() => v.key.into(),
						})
					}
				},
			},
			"issuer".to_string(), if let Some(v) = self.issue => Value::from(map!{
				"alg".to_string() => v.alg.to_string().into(),
				"key".to_string() => "[REDACTED]".into(),
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
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, "{}", self)
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AccessDefinition {
	pub(crate) name: String,
	pub(crate) access_type: AccessType,
	pub(crate) base: Base,
	pub(crate) authenticate: Option<Expr>,
	pub(crate) grant_duration: Option<Duration>,
	pub(crate) token_duration: Option<Duration>,
	pub(crate) session_duration: Option<Duration>,
	pub(crate) comment: Option<String>,
}
impl_kv_value_revisioned!(AccessDefinition);

impl AccessDefinition {
	fn to_sql_definition(&self) -> crate::sql::statements::define::DefineAccessStatement {
		// Create a redacted version of the access type
		let redacted_access_type = self.access_type.clone().redacted();

		crate::sql::statements::define::DefineAccessStatement {
			kind: crate::sql::statements::define::DefineKind::Default,
			name: crate::sql::Expr::Idiom(crate::sql::Idiom::field(self.name.clone())),
			access_type: crate::sql::AccessType::from(crate::expr::AccessType::from(
				redacted_access_type,
			)),
			authenticate: self.authenticate.clone().map(|e| e.into()),
			duration: crate::sql::access::AccessDuration {
				grant: self.grant_duration.map(|d| {
					crate::sql::Expr::Literal(crate::sql::Literal::Duration(
						crate::types::PublicDuration::from(d),
					))
				}),
				token: self.token_duration.map(|d| {
					crate::sql::Expr::Literal(crate::sql::Literal::Duration(
						crate::types::PublicDuration::from(d),
					))
				}),
				session: self.session_duration.map(|d| {
					crate::sql::Expr::Literal(crate::sql::Literal::Duration(
						crate::types::PublicDuration::from(d),
					))
				}),
			},
			comment: self
				.comment
				.clone()
				.map(|c| crate::sql::Expr::Literal(crate::sql::Literal::String(c))),
			base: crate::sql::Base::from(crate::expr::Base::from(self.base.clone())),
		}
	}
}

// Redaction methods for access types
impl AccessType {
	fn redacted(self) -> Self {
		match self {
			AccessType::Jwt(jwt) => AccessType::Jwt(jwt.redacted()),
			AccessType::Record(mut rec) => {
				rec.jwt = rec.jwt.redacted();
				if let Some(bearer) = rec.bearer {
					rec.bearer = Some(bearer.redacted());
				}
				AccessType::Record(rec)
			}
			AccessType::Bearer(mut bearer) => {
				bearer.jwt = bearer.jwt.redacted();
				AccessType::Bearer(bearer)
			}
		}
	}
}

impl JwtAccess {
	fn redacted(self) -> Self {
		Self {
			verify: self.verify.redacted(),
			issue: self.issue.map(|i| i.redacted()),
		}
	}
}

impl JwtAccessVerify {
	fn redacted(self) -> Self {
		match self {
			Self::Key(mut k) => {
				// Redact symmetric keys
				if k.alg.is_symmetric() {
					k.key = "[REDACTED]".to_string();
				}
				Self::Key(k)
			}
			Self::Jwks(j) => Self::Jwks(j),
		}
	}
}

impl JwtAccessIssue {
	fn redacted(self) -> Self {
		Self {
			alg: self.alg,
			// Always redact issuer keys as they're private keys
			key: "[REDACTED]".to_string(),
		}
	}
}

impl BearerAccess {
	fn redacted(self) -> Self {
		Self {
			kind: self.kind,
			subject: self.subject,
			jwt: self.jwt.redacted(),
		}
	}
}

impl InfoStructure for AccessDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.into(),
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

impl ToSql for AccessDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_sql_definition().fmt_sql(f, fmt)
	}
}

// Conversions between catalog and expr types
impl From<AccessType> for crate::expr::AccessType {
	fn from(v: AccessType) -> Self {
		match v {
			AccessType::Record(v) => Self::Record(v.into()),
			AccessType::Jwt(v) => Self::Jwt(v.into()),
			AccessType::Bearer(v) => Self::Bearer(v.into()),
		}
	}
}

impl From<crate::expr::AccessType> for AccessType {
	fn from(v: crate::expr::AccessType) -> Self {
		match v {
			crate::expr::AccessType::Record(v) => AccessType::Record(v.into()),
			crate::expr::AccessType::Jwt(v) => AccessType::Jwt(v.into()),
			crate::expr::AccessType::Bearer(v) => AccessType::Bearer(v.into()),
		}
	}
}

impl From<RecordAccess> for crate::expr::RecordAccess {
	fn from(v: RecordAccess) -> Self {
		Self {
			signup: v.signup,
			signin: v.signin,
			jwt: v.jwt.into(),
			bearer: v.bearer.map(|b| b.into()),
		}
	}
}

impl From<crate::expr::RecordAccess> for RecordAccess {
	fn from(v: crate::expr::RecordAccess) -> Self {
		Self {
			signup: v.signup,
			signin: v.signin,
			jwt: v.jwt.into(),
			bearer: v.bearer.map(|b| b.into()),
		}
	}
}

impl From<JwtAccess> for crate::expr::JwtAccess {
	fn from(v: JwtAccess) -> Self {
		Self {
			verify: v.verify.into(),
			issue: v.issue.map(|i| i.into()),
		}
	}
}

impl From<crate::expr::JwtAccess> for JwtAccess {
	fn from(v: crate::expr::JwtAccess) -> Self {
		Self {
			verify: v.verify.into(),
			issue: v.issue.map(|i| i.into()),
		}
	}
}

impl From<JwtAccessVerify> for crate::expr::access_type::JwtAccessVerify {
	fn from(v: JwtAccessVerify) -> Self {
		match v {
			JwtAccessVerify::Key(k) => Self::Key(k.into()),
			JwtAccessVerify::Jwks(j) => Self::Jwks(j.into()),
		}
	}
}

impl From<crate::expr::access_type::JwtAccessVerify> for JwtAccessVerify {
	fn from(v: crate::expr::access_type::JwtAccessVerify) -> Self {
		match v {
			crate::expr::access_type::JwtAccessVerify::Key(k) => JwtAccessVerify::Key(k.into()),
			crate::expr::access_type::JwtAccessVerify::Jwks(j) => JwtAccessVerify::Jwks(j.into()),
		}
	}
}

impl From<JwtAccessVerifyKey> for crate::expr::access_type::JwtAccessVerifyKey {
	fn from(v: JwtAccessVerifyKey) -> Self {
		Self {
			alg: v.alg.into(),
			key: crate::expr::Expr::Literal(crate::expr::Literal::String(v.key)),
		}
	}
}

impl From<crate::expr::access_type::JwtAccessVerifyKey> for JwtAccessVerifyKey {
	fn from(v: crate::expr::access_type::JwtAccessVerifyKey) -> Self {
		Self {
			alg: v.alg.into(),
			key: match v.key {
				crate::expr::Expr::Literal(crate::expr::Literal::String(s)) => s,
				_ => v.key.to_string(),
			},
		}
	}
}

impl From<JwtAccessVerifyJwks> for crate::expr::access_type::JwtAccessVerifyJwks {
	fn from(v: JwtAccessVerifyJwks) -> Self {
		Self {
			url: crate::expr::Expr::Literal(crate::expr::Literal::String(v.url)),
		}
	}
}

impl From<crate::expr::access_type::JwtAccessVerifyJwks> for JwtAccessVerifyJwks {
	fn from(v: crate::expr::access_type::JwtAccessVerifyJwks) -> Self {
		Self {
			url: match v.url {
				crate::expr::Expr::Literal(crate::expr::Literal::String(s)) => s,
				_ => v.url.to_string(),
			},
		}
	}
}

impl From<JwtAccessIssue> for crate::expr::access_type::JwtAccessIssue {
	fn from(v: JwtAccessIssue) -> Self {
		Self {
			alg: v.alg.into(),
			key: crate::expr::Expr::Literal(crate::expr::Literal::String(v.key)),
		}
	}
}

impl From<crate::expr::access_type::JwtAccessIssue> for JwtAccessIssue {
	fn from(v: crate::expr::access_type::JwtAccessIssue) -> Self {
		Self {
			alg: v.alg.into(),
			key: match v.key {
				crate::expr::Expr::Literal(crate::expr::Literal::String(s)) => s,
				_ => v.key.to_string(),
			},
		}
	}
}

impl From<BearerAccess> for crate::expr::access_type::BearerAccess {
	fn from(v: BearerAccess) -> Self {
		Self {
			kind: v.kind.into(),
			subject: v.subject.into(),
			jwt: v.jwt.into(),
		}
	}
}

impl From<crate::expr::access_type::BearerAccess> for BearerAccess {
	fn from(v: crate::expr::access_type::BearerAccess) -> Self {
		Self {
			kind: v.kind.into(),
			subject: v.subject.into(),
			jwt: v.jwt.into(),
		}
	}
}

impl From<BearerAccessType> for crate::expr::access_type::BearerAccessType {
	fn from(v: BearerAccessType) -> Self {
		match v {
			BearerAccessType::Bearer => Self::Bearer,
			BearerAccessType::Refresh => Self::Refresh,
		}
	}
}

impl From<crate::expr::access_type::BearerAccessType> for BearerAccessType {
	fn from(v: crate::expr::access_type::BearerAccessType) -> Self {
		match v {
			crate::expr::access_type::BearerAccessType::Bearer => Self::Bearer,
			crate::expr::access_type::BearerAccessType::Refresh => Self::Refresh,
		}
	}
}

impl From<BearerAccessSubject> for crate::expr::access_type::BearerAccessSubject {
	fn from(v: BearerAccessSubject) -> Self {
		match v {
			BearerAccessSubject::Record => Self::Record,
			BearerAccessSubject::User => Self::User,
		}
	}
}

impl From<crate::expr::access_type::BearerAccessSubject> for BearerAccessSubject {
	fn from(v: crate::expr::access_type::BearerAccessSubject) -> Self {
		match v {
			crate::expr::access_type::BearerAccessSubject::Record => Self::Record,
			crate::expr::access_type::BearerAccessSubject::User => Self::User,
		}
	}
}

impl From<Algorithm> for crate::expr::Algorithm {
	fn from(v: Algorithm) -> Self {
		match v {
			Algorithm::EdDSA => Self::EdDSA,
			Algorithm::Es256 => Self::Es256,
			Algorithm::Es384 => Self::Es384,
			Algorithm::Es512 => Self::Es512,
			Algorithm::Hs256 => Self::Hs256,
			Algorithm::Hs384 => Self::Hs384,
			Algorithm::Hs512 => Self::Hs512,
			Algorithm::Ps256 => Self::Ps256,
			Algorithm::Ps384 => Self::Ps384,
			Algorithm::Ps512 => Self::Ps512,
			Algorithm::Rs256 => Self::Rs256,
			Algorithm::Rs384 => Self::Rs384,
			Algorithm::Rs512 => Self::Rs512,
		}
	}
}

impl From<crate::expr::Algorithm> for Algorithm {
	fn from(v: crate::expr::Algorithm) -> Self {
		match v {
			crate::expr::Algorithm::EdDSA => Self::EdDSA,
			crate::expr::Algorithm::Es256 => Self::Es256,
			crate::expr::Algorithm::Es384 => Self::Es384,
			crate::expr::Algorithm::Es512 => Self::Es512,
			crate::expr::Algorithm::Hs256 => Self::Hs256,
			crate::expr::Algorithm::Hs384 => Self::Hs384,
			crate::expr::Algorithm::Hs512 => Self::Hs512,
			crate::expr::Algorithm::Ps256 => Self::Ps256,
			crate::expr::Algorithm::Ps384 => Self::Ps384,
			crate::expr::Algorithm::Ps512 => Self::Ps512,
			crate::expr::Algorithm::Rs256 => Self::Rs256,
			crate::expr::Algorithm::Rs384 => Self::Rs384,
			crate::expr::Algorithm::Rs512 => Self::Rs512,
		}
	}
}
