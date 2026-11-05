use common::fmt::{EscapeIdent, EscapeKwFreeIdent, SqlDuration};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{Base, Cond, RecordIdLit};
use crate::val::Duration;

// Keys and their identifiers are generated randomly from a 62-character pool.
pub static GRANT_BEARER_CHARACTER_POOL: &[u8] =
	b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
// The key identifier should not have collisions to prevent confusion.
// However, collisions should be handled gracefully when issuing grants.
// The first character of the key identifier will not be a digit to prevent parsing issues.
// With 12 characters from the pool, one alphabetic, the key identifier part has ~68 bits of
// entropy.
pub static GRANT_BEARER_ID_LENGTH: usize = 12;
// With 24 characters from the pool, the key part has ~140 bits of entropy.
pub static GRANT_BEARER_KEY_LENGTH: usize = 24;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AccessStatement {
	Grant(AccessStatementGrant),   // Create access grant.
	Show(AccessStatementShow),     // Show access grants.
	Revoke(AccessStatementRevoke), // Revoke access grant.
	Purge(AccessStatementPurge),   // Purge access grants.
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AccessStatementGrant {
	pub ac: Strand,
	pub base: Option<Base>,
	pub subject: Subject,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AccessStatementShow {
	pub ac: Strand,
	pub base: Option<Base>,
	pub gr: Option<Strand>,
	pub cond: Option<Cond>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AccessStatementRevoke {
	pub ac: Strand,
	pub base: Option<Base>,
	pub gr: Option<Strand>,
	pub cond: Option<Cond>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AccessStatementPurge {
	pub ac: Strand,
	pub base: Option<Base>,
	pub kind: PurgeKind,
	pub grace: Duration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PurgeKind {
	#[default]
	Expired,
	Revoked,
	Both,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
					write_sql!(f, fmt, " FOR {}", SqlDuration(stmt.grace.0));
				}
			}
		}
	}
}
