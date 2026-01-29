use std::str::FromStr;

use anyhow::Result;

use crate::err::Error;
use crate::expr::statements::DefineAccessStatement;
use crate::expr::{Algorithm, Expr, Literal};

/// The type of access methods available

#[derive(Debug, Hash, Clone, Eq, PartialEq, priority_lfu::DeepSizeOf)]
pub(crate) enum AccessType {
	Record(RecordAccess),
	Jwt(JwtAccess),
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

#[derive(Debug, Hash, Clone, Eq, PartialEq, priority_lfu::DeepSizeOf)]
pub(crate) struct JwtAccess {
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
				key: Expr::Literal(Literal::String(key.clone())),
			}),
			issue: Some(JwtAccessIssue {
				alg,
				key: Expr::Literal(Literal::String(key)),
			}),
		}
	}
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, priority_lfu::DeepSizeOf)]
pub(crate) struct JwtAccessIssue {
	pub(crate) alg: Algorithm,
	pub(crate) key: Expr,
}

impl Default for JwtAccessIssue {
	fn default() -> Self {
		Self {
			// Defaults to HS512
			alg: Algorithm::Hs512,
			// Avoid defaulting to empty key
			key: Expr::Literal(Literal::String(DefineAccessStatement::random_key())),
		}
	}
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, priority_lfu::DeepSizeOf)]
pub(crate) enum JwtAccessVerify {
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
#[derive(Debug, Hash, Clone, Eq, PartialEq, priority_lfu::DeepSizeOf)]
pub(crate) struct JwtAccessVerifyKey {
	pub(crate) alg: Algorithm,
	pub(crate) key: Expr,
}

impl Default for JwtAccessVerifyKey {
	fn default() -> Self {
		Self {
			// Defaults to HS512
			alg: Algorithm::Hs512,
			// Avoid defaulting to empty key
			key: Expr::Literal(Literal::String(DefineAccessStatement::random_key())),
		}
	}
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, priority_lfu::DeepSizeOf)]
pub(crate) struct JwtAccessVerifyJwks {
	pub(crate) url: Expr,
}

#[derive(Debug, Hash, Clone, Eq, PartialEq, priority_lfu::DeepSizeOf)]
pub(crate) struct RecordAccess {
	pub signup: Option<Expr>,
	pub signin: Option<Expr>,
	pub jwt: JwtAccess,
	pub bearer: Option<BearerAccess>,
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

#[derive(Debug, Hash, Clone, Eq, PartialEq, priority_lfu::DeepSizeOf)]
pub(crate) struct BearerAccess {
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

#[derive(Debug, Hash, Clone, Eq, PartialEq, priority_lfu::DeepSizeOf)]
pub(crate) enum BearerAccessType {
	Bearer,
	Refresh,
}

impl BearerAccessType {
	#[cfg(test)]
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

#[derive(Debug, Hash, Clone, Eq, PartialEq, priority_lfu::DeepSizeOf)]
pub enum BearerAccessSubject {
	Record,
	User,
}
