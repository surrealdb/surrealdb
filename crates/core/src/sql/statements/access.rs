use std::fmt;
use std::fmt::{Display, Formatter};

use crate::sql::{Base, Cond, Ident, RecordIdLit};
use crate::val::{Datetime, Duration, Strand, Uuid};

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
	pub ac: Ident,
	pub base: Option<Base>,
	pub subject: Subject,
}

impl From<AccessStatementGrant> for crate::expr::statements::access::AccessStatementGrant {
	fn from(v: AccessStatementGrant) -> Self {
		Self {
			ac: v.ac.into(),
			base: v.base.map(Into::into),
			subject: v.subject.into(),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementGrant> for AccessStatementGrant {
	fn from(v: crate::expr::statements::access::AccessStatementGrant) -> Self {
		Self {
			ac: v.ac.into(),
			base: v.base.map(Into::into),
			subject: v.subject.into(),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementShow {
	pub ac: Ident,
	pub base: Option<Base>,
	pub gr: Option<Ident>,
	pub cond: Option<Cond>,
}

impl From<AccessStatementShow> for crate::expr::statements::access::AccessStatementShow {
	fn from(v: AccessStatementShow) -> Self {
		Self {
			ac: v.ac.into(),
			base: v.base.map(Into::into),
			gr: v.gr.map(Into::into),
			cond: v.cond.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementShow> for AccessStatementShow {
	fn from(v: crate::expr::statements::access::AccessStatementShow) -> Self {
		Self {
			ac: v.ac.into(),
			base: v.base.map(Into::into),
			gr: v.gr.map(Into::into),
			cond: v.cond.map(Into::into),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementRevoke {
	pub ac: Ident,
	pub base: Option<Base>,
	pub gr: Option<Ident>,
	pub cond: Option<Cond>,
}

impl From<AccessStatementRevoke> for crate::expr::statements::access::AccessStatementRevoke {
	fn from(v: AccessStatementRevoke) -> Self {
		Self {
			ac: v.ac.into(),
			base: v.base.map(Into::into),
			gr: v.gr.map(Into::into),
			cond: v.cond.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementRevoke> for AccessStatementRevoke {
	fn from(v: crate::expr::statements::access::AccessStatementRevoke) -> Self {
		Self {
			ac: v.ac.into(),
			base: v.base.map(Into::into),
			gr: v.gr.map(Into::into),
			cond: v.cond.map(Into::into),
		}
	}
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessStatementPurge {
	pub ac: Ident,
	pub base: Option<Base>,
	// TODO: Merge these booleans into a enum as having them both be false is invalid state.
	pub expired: bool,
	pub revoked: bool,
	pub grace: Duration,
}

impl From<AccessStatementPurge> for crate::expr::statements::access::AccessStatementPurge {
	fn from(v: AccessStatementPurge) -> Self {
		Self {
			ac: v.ac.into(),
			base: v.base.map(Into::into),
			expired: v.expired,
			revoked: v.revoked,
			grace: v.grace,
		}
	}
}

impl From<crate::expr::statements::access::AccessStatementPurge> for AccessStatementPurge {
	fn from(v: crate::expr::statements::access::AccessStatementPurge) -> Self {
		Self {
			ac: v.ac.into(),
			base: v.base.map(Into::into),
			expired: v.expired,
			revoked: v.revoked,
			grace: v.grace,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AccessGrant {
	pub id: Ident,                    // Unique grant identifier.
	pub ac: Ident,                    // Access method used to create the grant.
	pub creation: Datetime,           // Grant creation time.
	pub expiration: Option<Datetime>, // Grant expiration time, if any.
	pub revocation: Option<Datetime>, // Grant revocation time, if any.
	pub subject: Subject,             // Subject of the grant.
	pub grant: Grant,                 // Grant data.
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Subject {
	Record(RecordIdLit),
	User(Ident),
}

impl From<Subject> for crate::expr::statements::access::Subject {
	fn from(v: Subject) -> Self {
		match v {
			Subject::Record(id) => Self::Record(id.into()),
			Subject::User(name) => Self::User(name.into()),
		}
	}
}

impl From<crate::expr::statements::access::Subject> for Subject {
	fn from(v: crate::expr::statements::access::Subject) -> Self {
		match v {
			crate::expr::statements::access::Subject::Record(id) => Self::Record(id.into()),
			crate::expr::statements::access::Subject::User(name) => Self::User(name.into()),
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Grant {
	Jwt(GrantJwt),
	Record(GrantRecord),
	Bearer(GrantBearer),
}

impl Grant {
	// Returns the type of the grant as a string.
	pub fn variant(&self) -> &str {
		match self {
			Grant::Jwt(_) => "jwt",
			Grant::Record(_) => "record",
			Grant::Bearer(_) => "bearer",
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct GrantJwt {
	pub jti: Uuid,             // JWT ID
	pub token: Option<Strand>, // JWT. Will not be stored after being returned.
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct GrantRecord {
	pub rid: Uuid,             // Record ID
	pub jti: Uuid,             // JWT ID
	pub token: Option<Strand>, // JWT. Will not be stored after being returned.
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct GrantBearer {
	pub id: Ident, // Key ID
	// Key. Will not be stored and be returned as redacted.
	// Immediately after generation, it will contain the plaintext key.
	// Will be hashed before storage so that the plaintext key is not stored.
	pub key: Strand,
}

impl Display for AccessStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Grant(stmt) => {
				write!(f, "ACCESS {}", stmt.ac)?;
				if let Some(ref v) = stmt.base {
					write!(f, " ON {v}")?;
				}
				write!(f, " GRANT")?;
				match &stmt.subject {
					Subject::User(x) => write!(f, " FOR USER {}", x)?,
					Subject::Record(x) => write!(f, " FOR RECORD {}", x)?,
				}
				Ok(())
			}
			Self::Show(stmt) => {
				write!(f, "ACCESS {}", stmt.ac)?;
				if let Some(ref v) = stmt.base {
					write!(f, " ON {v}")?;
				}
				write!(f, " SHOW")?;
				match &stmt.gr {
					Some(v) => write!(f, " GRANT {v}")?,
					None => match &stmt.cond {
						Some(v) => write!(f, " {v}")?,
						None => write!(f, " ALL")?,
					},
				};
				Ok(())
			}
			Self::Revoke(stmt) => {
				write!(f, "ACCESS {}", stmt.ac)?;
				if let Some(ref v) = stmt.base {
					write!(f, " ON {v}")?;
				}
				write!(f, " REVOKE")?;
				match &stmt.gr {
					Some(v) => write!(f, " GRANT {v}")?,
					None => match &stmt.cond {
						Some(v) => write!(f, " {v}")?,
						None => write!(f, " ALL")?,
					},
				};
				Ok(())
			}
			Self::Purge(stmt) => {
				write!(f, "ACCESS {}", stmt.ac)?;
				if let Some(ref v) = stmt.base {
					write!(f, " ON {v}")?;
				}
				write!(f, " PURGE")?;
				match (stmt.expired, stmt.revoked) {
					(true, false) => write!(f, " EXPIRED")?,
					(false, true) => write!(f, " REVOKED")?,
					(true, true) => write!(f, " EXPIRED, REVOKED")?,
					// This case should not parse.
					(false, false) => write!(f, " NONE")?,
				};
				if !stmt.grace.is_zero() {
					write!(f, " FOR {}", stmt.grace)?;
				}
				Ok(())
			}
		}
	}
}
