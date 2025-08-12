use std::fmt;
use std::fmt::Display;
use std::str::FromStr;

use anyhow::Result;
use rand::Rng;
use rand::distributions::Alphanumeric;

use super::Expr;
use crate::err::Error;
use crate::sql::Algorithm;
use crate::sql::escape::QuoteStr;

pub(crate) fn random_key() -> String {
	rand::thread_rng().sample_iter(&Alphanumeric).take(128).map(char::from).collect::<String>()
}

/// The type of access methods available
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AccessType {
	Record(RecordAccess),
	Jwt(JwtAccess),
	// TODO(gguillemas): Document once bearer access is no longer experimental.
	Bearer(BearerAccess),
}

impl Default for AccessType {
	fn default() -> Self {
		// Access type defaults to the most specific
		Self::Record(RecordAccess {
			..Default::default()
		})
	}
}

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

// Allows retrieving the JWT configuration for any access type.
pub trait Jwt {
	fn jwt(&self) -> &JwtAccess;
}

impl Jwt for AccessType {
	fn jwt(&self) -> &JwtAccess {
		match self {
			AccessType::Record(at) => &at.jwt,
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

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct JwtAccess {
	// Verify is required
	pub verify: JwtAccessVerify,
	// Issue is optional
	// It is possible to only verify externally issued tokens
	pub issue: Option<JwtAccessIssue>,
}

//TODO: Move this logic out of the parser
impl Default for JwtAccess {
	fn default() -> Self {
		// Defaults to HS512 with a randomly generated key
		let alg = Algorithm::Hs512;
		let key = random_key();
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

impl From<JwtAccess> for crate::expr::JwtAccess {
	fn from(v: JwtAccess) -> Self {
		Self {
			verify: v.verify.into(),
			issue: v.issue.map(Into::into),
		}
	}
}

impl From<crate::expr::JwtAccess> for JwtAccess {
	fn from(v: crate::expr::JwtAccess) -> Self {
		Self {
			verify: v.verify.into(),
			issue: v.issue.map(Into::into),
		}
	}
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct JwtAccessIssue {
	pub alg: Algorithm,
	pub key: String,
}

impl Default for JwtAccessIssue {
	fn default() -> Self {
		// TODO: Move this computation out of the AST
		Self {
			// Defaults to HS512
			alg: Algorithm::Hs512,
			// Avoid defaulting to empty key
			key: random_key(),
		}
	}
}

impl From<JwtAccessIssue> for crate::expr::access_type::JwtAccessIssue {
	fn from(v: JwtAccessIssue) -> Self {
		Self {
			alg: v.alg.into(),
			key: v.key,
		}
	}
}

impl From<crate::expr::access_type::JwtAccessIssue> for JwtAccessIssue {
	fn from(v: crate::expr::access_type::JwtAccessIssue) -> Self {
		Self {
			alg: v.alg.into(),
			key: v.key,
		}
	}
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum JwtAccessVerify {
	Key(JwtAccessVerifyKey),
	Jwks(JwtAccessVerifyJwks),
}

impl From<JwtAccessVerify> for crate::expr::access_type::JwtAccessVerify {
	fn from(v: JwtAccessVerify) -> Self {
		match v {
			JwtAccessVerify::Key(v) => Self::Key(v.into()),
			JwtAccessVerify::Jwks(v) => Self::Jwks(v.into()),
		}
	}
}

impl From<crate::expr::access_type::JwtAccessVerify> for JwtAccessVerify {
	fn from(v: crate::expr::access_type::JwtAccessVerify) -> Self {
		match v {
			crate::expr::access_type::JwtAccessVerify::Key(v) => Self::Key(v.into()),
			crate::expr::access_type::JwtAccessVerify::Jwks(v) => Self::Jwks(v.into()),
		}
	}
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct JwtAccessVerifyKey {
	pub alg: Algorithm,
	pub key: String,
}

impl From<JwtAccessVerifyKey> for crate::expr::access_type::JwtAccessVerifyKey {
	fn from(v: JwtAccessVerifyKey) -> Self {
		Self {
			alg: v.alg.into(),
			key: v.key,
		}
	}
}

impl From<crate::expr::access_type::JwtAccessVerifyKey> for JwtAccessVerifyKey {
	fn from(v: crate::expr::access_type::JwtAccessVerifyKey) -> Self {
		Self {
			alg: v.alg.into(),
			key: v.key,
		}
	}
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct JwtAccessVerifyJwks {
	pub url: String,
}

impl From<JwtAccessVerifyJwks> for crate::expr::access_type::JwtAccessVerifyJwks {
	fn from(v: JwtAccessVerifyJwks) -> Self {
		Self {
			url: v.url,
		}
	}
}

impl From<crate::expr::access_type::JwtAccessVerifyJwks> for JwtAccessVerifyJwks {
	fn from(v: crate::expr::access_type::JwtAccessVerifyJwks) -> Self {
		Self {
			url: v.url,
		}
	}
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordAccess {
	pub signup: Option<Expr>,
	pub signin: Option<Expr>,
	pub jwt: JwtAccess,
	pub bearer: Option<BearerAccess>,
}

impl From<RecordAccess> for crate::expr::RecordAccess {
	fn from(v: RecordAccess) -> Self {
		Self {
			signup: v.signup.map(Into::into),
			signin: v.signin.map(Into::into),
			jwt: v.jwt.into(),
			bearer: v.bearer.map(Into::into),
		}
	}
}

impl From<crate::expr::RecordAccess> for RecordAccess {
	fn from(v: crate::expr::RecordAccess) -> Self {
		Self {
			signup: v.signup.map(Into::into),
			signin: v.signin.map(Into::into),
			jwt: v.jwt.into(),
			bearer: v.bearer.map(Into::into),
		}
	}
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
			jwt: JwtAccess::default(),
		}
	}
}

impl Jwt for BearerAccess {
	fn jwt(&self) -> &JwtAccess {
		&self.jwt
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

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum BearerAccessSubject {
	Record,
	User,
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
