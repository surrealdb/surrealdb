use rand::distr::{Alphanumeric, SampleString};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::Expr;
use crate::{Algorithm, CoverStmts, Literal};

pub fn random_key() -> String {
	Alphanumeric.sample_string(&mut rand::rng(), 128)
}

/// The type of access methods available
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AccessType {
	Record(Box<RecordAccess>),
	Jwt(JwtAccess),
	Bearer(BearerAccess),
}

impl Default for AccessType {
	fn default() -> Self {
		// Access type defaults to the most specific
		Self::Record(Box::new(RecordAccess {
			..Default::default()
		}))
	}
}

impl ToSql for AccessType {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			AccessType::Jwt(ac) => {
				write_sql!(f, sql_fmt, "JWT {}", ac);
			}
			AccessType::Record(ac) => {
				write_sql!(f, sql_fmt, "RECORD");
				if let Some(ref v) = ac.signup {
					write_sql!(f, sql_fmt, " SIGNUP {}", CoverStmts(v));
				}
				if let Some(ref v) = ac.signin {
					write_sql!(f, sql_fmt, " SIGNIN {}", CoverStmts(v));
				}
				if ac.bearer.is_some() {
					write_sql!(f, sql_fmt, " WITH REFRESH")
				}
				write_sql!(f, sql_fmt, " WITH JWT {}", ac.jwt);
			}
			AccessType::Bearer(ac) => {
				write_sql!(f, sql_fmt, "BEARER");
				match ac.subject {
					BearerAccessSubject::User => write_sql!(f, sql_fmt, " FOR USER"),
					BearerAccessSubject::Record => write_sql!(f, sql_fmt, " FOR RECORD"),
				}
			}
		}
	}
}

impl AccessType {
	/// Returns whether or not the access method can issue non-token grants
	/// In this context, token refers exclusively to JWT
	#[allow(dead_code)]
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
	#[allow(dead_code)]
	pub fn can_issue_tokens(&self) -> bool {
		match self {
			// The JWT access method can only issue tokens if an issuer is set
			AccessType::Jwt(jwt) => jwt.issue.is_some(),
			_ => true,
		}
	}
}

#[derive(Debug, Clone, Eq, PartialEq)]
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
				key: Expr::Literal(Literal::String(key.as_str().into())),
			}),
			issue: Some(JwtAccessIssue {
				alg,
				key: Expr::Literal(Literal::String(key.into())),
			}),
		}
	}
}

impl ToSql for JwtAccess {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match &self.verify {
			JwtAccessVerify::Key(v) => {
				write_sql!(f, sql_fmt, "ALGORITHM {} KEY {}", v.alg, CoverStmts(&v.key));
			}
			JwtAccessVerify::Jwks(v) => {
				write_sql!(f, sql_fmt, "URL {}", CoverStmts(&v.url));
			}
		}
		if let Some(iss) = &self.issue {
			write_sql!(f, sql_fmt, " WITH ISSUER KEY {}", CoverStmts(&iss.key));
		}
	}
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct JwtAccessIssue {
	pub alg: Algorithm,
	pub key: Expr,
}

impl Default for JwtAccessIssue {
	fn default() -> Self {
		// TODO: Move this computation out of the AST
		Self {
			// Defaults to HS512
			alg: Algorithm::Hs512,
			// Avoid defaulting to empty key
			key: Expr::Literal(Literal::String(random_key().into())),
		}
	}
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum JwtAccessVerify {
	Key(JwtAccessVerifyKey),
	Jwks(JwtAccessVerifyJwks),
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct JwtAccessVerifyKey {
	pub alg: Algorithm,
	pub key: Expr,
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct JwtAccessVerifyJwks {
	pub url: Expr,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordAccess {
	pub signup: Option<Expr>,
	pub signin: Option<Expr>,
	pub jwt: JwtAccess,
	pub bearer: Option<BearerAccess>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
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

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum BearerAccessType {
	Bearer,
	Refresh,
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum BearerAccessSubject {
	Record,
	User,
}
