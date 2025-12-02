use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{EscapeIdent, EscapeKwFreeIdent};
use crate::sql::{Base, Cond, RecordIdLit};
use crate::types::PublicDuration;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AccessStatement {
	Grant(AccessStatementGrant),   // Create access grant.
	Show(AccessStatementShow),     // Show access grants.
	Revoke(AccessStatementRevoke), // Revoke access grant.
	Purge(AccessStatementPurge),   // Purge access grants.
}

impl From<AccessStatement> for crate::expr::statements::access::AccessStatement {
	fn from(v: AccessStatement) -> Self {
		match v {
			AccessStatement::Grant(v) => Self::Grant(v.into()),
			AccessStatement::Show(v) => Self::Show(v.into()),
			AccessStatement::Revoke(v) => Self::Revoke(v.into()),
			AccessStatement::Purge(v) => Self::Purge(v.into()),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatement> for AccessStatement {
	fn from(v: crate::expr::statements::access::AccessStatement) -> Self {
		match v {
			crate::expr::statements::access::AccessStatement::Grant(v) => Self::Grant(v.into()),
			crate::expr::statements::access::AccessStatement::Show(v) => Self::Show(v.into()),
			crate::expr::statements::access::AccessStatement::Revoke(v) => Self::Revoke(v.into()),
			crate::expr::statements::access::AccessStatement::Purge(v) => Self::Purge(v.into()),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementGrant {
	pub ac: String,
	pub base: Option<Base>,
	pub subject: Subject,
}

impl From<AccessStatementGrant> for crate::expr::statements::access::AccessStatementGrant {
	fn from(v: AccessStatementGrant) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			subject: v.subject.into(),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementGrant> for AccessStatementGrant {
	fn from(v: crate::expr::statements::access::AccessStatementGrant) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			subject: v.subject.into(),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementShow {
	pub ac: String,
	pub base: Option<Base>,
	pub gr: Option<String>,
	pub cond: Option<Cond>,
}

impl From<AccessStatementShow> for crate::expr::statements::access::AccessStatementShow {
	fn from(v: AccessStatementShow) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			gr: v.gr,
			cond: v.cond.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementShow> for AccessStatementShow {
	fn from(v: crate::expr::statements::access::AccessStatementShow) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			gr: v.gr,
			cond: v.cond.map(Into::into),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementRevoke {
	pub ac: String,
	pub base: Option<Base>,
	pub gr: Option<String>,
	pub cond: Option<Cond>,
}

impl From<AccessStatementRevoke> for crate::expr::statements::access::AccessStatementRevoke {
	fn from(v: AccessStatementRevoke) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			gr: v.gr,
			cond: v.cond.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementRevoke> for AccessStatementRevoke {
	fn from(v: crate::expr::statements::access::AccessStatementRevoke) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(Into::into),
			gr: v.gr,
			cond: v.cond.map(Into::into),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementPurge {
	pub ac: String,
	pub base: Option<Base>,
	pub kind: PurgeKind,
	pub grace: PublicDuration,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PurgeKind {
	#[default]
	Expired,
	Revoked,
	Both,
}

impl From<AccessStatementPurge> for crate::expr::statements::access::AccessStatementPurge {
	fn from(v: AccessStatementPurge) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(From::from),
			kind: v.kind.into(),
			grace: v.grace.into(),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementPurge> for AccessStatementPurge {
	fn from(v: crate::expr::statements::access::AccessStatementPurge) -> Self {
		Self {
			ac: v.ac,
			base: v.base.map(From::from),
			kind: v.kind.into(),
			grace: v.grace.into(),
		}
	}
}

impl From<PurgeKind> for crate::expr::statements::access::PurgeKind {
	fn from(v: PurgeKind) -> Self {
		match v {
			PurgeKind::Expired => crate::expr::statements::access::PurgeKind::Expired,
			PurgeKind::Revoked => crate::expr::statements::access::PurgeKind::Revoked,
			PurgeKind::Both => crate::expr::statements::access::PurgeKind::Both,
		}
	}
}

impl From<crate::expr::statements::access::PurgeKind> for PurgeKind {
	fn from(v: crate::expr::statements::access::PurgeKind) -> Self {
		match v {
			crate::expr::statements::access::PurgeKind::Expired => PurgeKind::Expired,
			crate::expr::statements::access::PurgeKind::Revoked => PurgeKind::Revoked,
			crate::expr::statements::access::PurgeKind::Both => PurgeKind::Both,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Subject {
	Record(RecordIdLit),
	User(String),
}

impl From<Subject> for crate::expr::statements::access::Subject {
	fn from(v: Subject) -> Self {
		match v {
			Subject::Record(id) => Self::Record(id.into()),
			Subject::User(name) => Self::User(name),
		}
	}
}

impl From<crate::expr::statements::access::Subject> for Subject {
	fn from(v: crate::expr::statements::access::Subject) -> Self {
		match v {
			crate::expr::statements::access::Subject::Record(id) => Self::Record(id.into()),
			crate::expr::statements::access::Subject::User(name) => Self::User(name),
		}
	}
}

impl ToSql for AccessStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Grant(stmt) => {
				write_sql!(f, fmt, "ACCESS {}", EscapeKwFreeIdent(&stmt.ac));
				if let Some(ref v) = stmt.base {
					write_sql!(f, fmt, " ON {v}");
				}
				write_sql!(f, fmt, " GRANT");
				match &stmt.subject {
					Subject::User(x) => write_sql!(f, fmt, " FOR USER {}", EscapeIdent(&x)),
					Subject::Record(x) => write_sql!(f, fmt, " FOR RECORD {}", x),
				}
			}
			Self::Show(stmt) => {
				write_sql!(f, fmt, "ACCESS {}", EscapeKwFreeIdent(&stmt.ac));
				if let Some(ref v) = stmt.base {
					write_sql!(f, fmt, " ON {v}");
				}
				write_sql!(f, fmt, " SHOW");
				match &stmt.gr {
					Some(v) => write_sql!(f, fmt, " GRANT {}", EscapeKwFreeIdent(v)),
					None => match &stmt.cond {
						Some(v) => write_sql!(f, fmt, " {v}"),
						None => write_sql!(f, fmt, " ALL"),
					},
				};
			}
			Self::Revoke(stmt) => {
				write_sql!(f, fmt, "ACCESS {}", EscapeKwFreeIdent(&stmt.ac));
				if let Some(ref v) = stmt.base {
					write_sql!(f, fmt, " ON {v}");
				}
				write_sql!(f, fmt, " REVOKE");
				match &stmt.gr {
					Some(v) => write_sql!(f, fmt, " GRANT {}", EscapeKwFreeIdent(v)),
					None => match &stmt.cond {
						Some(v) => write_sql!(f, fmt, " {v}"),
						None => write_sql!(f, fmt, " ALL"),
					},
				};
			}
			Self::Purge(stmt) => {
				write_sql!(f, fmt, "ACCESS {}", EscapeKwFreeIdent(&stmt.ac));
				if let Some(ref v) = stmt.base {
					write_sql!(f, fmt, " ON {v}");
				}
			}
		}
	}
}
