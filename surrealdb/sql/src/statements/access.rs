use common::fmt::{EscapeIdent, EscapeKwFreeIdent, SqlDuration};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{Base, Cond, RecordIdLit};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AccessStatement {
	Grant(AccessStatementGrant),   // Create access grant.
	Show(AccessStatementShow),     // Show access grants.
	Revoke(AccessStatementRevoke), // Revoke access grant.
	Purge(AccessStatementPurge),   // Purge access grants.
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementGrant {
	pub ac: Strand,
	pub base: Option<Base>,
	pub subject: Subject,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementShow {
	pub ac: Strand,
	pub base: Option<Base>,
	pub gr: Option<Strand>,
	pub cond: Option<Cond>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementRevoke {
	pub ac: Strand,
	pub base: Option<Base>,
	pub gr: Option<Strand>,
	pub cond: Option<Cond>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementPurge {
	pub ac: Strand,
	pub base: Option<Base>,
	pub kind: PurgeKind,
	pub grace: std::time::Duration,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PurgeKind {
	#[default]
	Expired,
	Revoked,
	Both,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Subject {
	Record(RecordIdLit),
	User(Strand),
}

impl ToSql for AccessStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Grant(stmt) => {
				write_sql!(f, fmt, "ACCESS {}", EscapeKwFreeIdent(stmt.ac.as_str()));
				if let Some(ref v) = stmt.base {
					write_sql!(f, fmt, " ON {v}");
				}
				write_sql!(f, fmt, " GRANT");
				match &stmt.subject {
					Subject::User(x) => write_sql!(f, fmt, " FOR USER {}", EscapeIdent(x.as_str())),
					Subject::Record(x) => write_sql!(f, fmt, " FOR RECORD {}", x),
				}
			}
			Self::Show(stmt) => {
				write_sql!(f, fmt, "ACCESS {}", EscapeKwFreeIdent(stmt.ac.as_str()));
				if let Some(ref v) = stmt.base {
					write_sql!(f, fmt, " ON {v}");
				}
				write_sql!(f, fmt, " SHOW");
				match &stmt.gr {
					Some(v) => write_sql!(f, fmt, " GRANT {}", EscapeKwFreeIdent(v.as_str())),
					None => match &stmt.cond {
						Some(v) => write_sql!(f, fmt, " {v}"),
						None => write_sql!(f, fmt, " ALL"),
					},
				};
			}
			Self::Revoke(stmt) => {
				write_sql!(f, fmt, "ACCESS {}", EscapeKwFreeIdent(stmt.ac.as_str()));
				if let Some(ref v) = stmt.base {
					write_sql!(f, fmt, " ON {v}");
				}
				write_sql!(f, fmt, " REVOKE");
				match &stmt.gr {
					Some(v) => write_sql!(f, fmt, " GRANT {}", EscapeKwFreeIdent(v.as_str())),
					None => match &stmt.cond {
						Some(v) => write_sql!(f, fmt, " {v}"),
						None => write_sql!(f, fmt, " ALL"),
					},
				};
			}
			Self::Purge(stmt) => {
				write_sql!(f, fmt, "ACCESS {}", EscapeKwFreeIdent(stmt.ac.as_str()));
				if let Some(ref v) = stmt.base {
					write_sql!(f, fmt, " ON {v}");
				}
				f.push_str(" PURGE");
				match stmt.kind {
					PurgeKind::Expired => f.push_str(" EXPIRED"),
					PurgeKind::Revoked => f.push_str(" REVOKED"),
					PurgeKind::Both => f.push_str(" EXPIRED, REVOKED"),
				}
				if !stmt.grace.is_zero() {
					write_sql!(f, fmt, " FOR {}", SqlDuration(stmt.grace));
				}
			}
		}
	}
}
