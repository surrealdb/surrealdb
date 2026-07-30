//! `sql` -> `expr` conversions for [`crate::sql::access_type`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::access_type::*;

impl From<AccessType> for crate::expr::AccessType {
	fn from(v: AccessType) -> Self {
		match v {
			AccessType::Record(v) => Self::Record(Box::new((*v).into())),
			AccessType::Jwt(v) => Self::Jwt(v.into()),
			AccessType::Bearer(v) => Self::Bearer(v.into()),
		}
	}
}

impl From<crate::expr::AccessType> for AccessType {
	fn from(v: crate::expr::AccessType) -> Self {
		match v {
			crate::expr::AccessType::Record(v) => AccessType::Record(Box::new((*v).into())),
			crate::expr::AccessType::Jwt(v) => AccessType::Jwt(v.into()),
			crate::expr::AccessType::Bearer(v) => AccessType::Bearer(v.into()),
		}
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

impl From<JwtAccessIssue> for crate::expr::access_type::JwtAccessIssue {
	fn from(v: JwtAccessIssue) -> Self {
		Self {
			alg: v.alg.into(),
			key: v.key.into(),
		}
	}
}

impl From<crate::expr::access_type::JwtAccessIssue> for JwtAccessIssue {
	fn from(v: crate::expr::access_type::JwtAccessIssue) -> Self {
		Self {
			alg: v.alg.into(),
			key: v.key.into(),
		}
	}
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

impl From<JwtAccessVerifyKey> for crate::expr::access_type::JwtAccessVerifyKey {
	fn from(v: JwtAccessVerifyKey) -> Self {
		Self {
			alg: v.alg.into(),
			key: v.key.into(),
		}
	}
}

impl From<crate::expr::access_type::JwtAccessVerifyKey> for JwtAccessVerifyKey {
	fn from(v: crate::expr::access_type::JwtAccessVerifyKey) -> Self {
		Self {
			alg: v.alg.into(),
			key: v.key.into(),
		}
	}
}

impl From<JwtAccessVerifyJwks> for crate::expr::access_type::JwtAccessVerifyJwks {
	fn from(v: JwtAccessVerifyJwks) -> Self {
		Self {
			url: v.url.into(),
		}
	}
}

impl From<crate::expr::access_type::JwtAccessVerifyJwks> for JwtAccessVerifyJwks {
	fn from(v: crate::expr::access_type::JwtAccessVerifyJwks) -> Self {
		Self {
			url: v.url.into(),
		}
	}
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
