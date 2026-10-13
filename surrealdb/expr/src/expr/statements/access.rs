use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

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
		let sql_stmt: crate::sql::statements::AccessStatement = self.clone().into();
		sql_stmt.fmt_sql(f, fmt);
	}
}
