use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::access_type::BearerAccessSubject;
use crate::sql::{
	AccessType, Array, Base, Cond, Datetime, Duration, FlowResultExt as _, Ident, Object, Strand,
	Thing, Uuid, SqlValue,
};
use crate::iam::{Action, ResourceKind};
use anyhow::{Result, bail, ensure};
use md5::Digest;
use rand::Rng;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::fmt;
use std::fmt::{Display, Formatter};

// Keys and their identifiers are generated randomly from a 62-character pool.
pub static GRANT_BEARER_CHARACTER_POOL: &[u8] =
	b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
// The key identifier should not have collisions to prevent confusion.
// However, collisions should be handled gracefully when issuing grants.
// The first character of the key identifier will not be a digit to prevent parsing issues.
// With 12 characters from the pool, one alphabetic, the key identifier part has ~68 bits of entropy.
pub static GRANT_BEARER_ID_LENGTH: usize = 12;
// With 24 characters from the pool, the key part has ~140 bits of entropy.
pub static GRANT_BEARER_KEY_LENGTH: usize = 24;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessStatementPurge {
	pub ac: Ident,
	pub base: Option<Base>,
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
			grace: v.grace.into(),
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
			grace: v.grace.into(),
		}
	}
}


#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessGrant {
	pub id: Ident,                    // Unique grant identifier.
	pub ac: Ident,                    // Access method used to create the grant.
	pub creation: Datetime,           // Grant creation time.
	pub expiration: Option<Datetime>, // Grant expiration time, if any.
	pub revocation: Option<Datetime>, // Grant revocation time, if any.
	pub subject: Subject,             // Subject of the grant.
	pub grant: Grant,                 // Grant data.
}

impl AccessGrant {
	/// Returns a version of the statement where potential secrets are redacted.
	/// This function should be used when displaying the statement to datastore users.
	/// This function should NOT be used when displaying the statement for export purposes.
	pub fn redacted(&self) -> AccessGrant {
		let mut ags = self.clone();
		ags.grant = match ags.grant {
			Grant::Jwt(mut gr) => {
				// Token should not even be stored. We clear it just as a precaution.
				gr.token = None;
				Grant::Jwt(gr)
			}
			Grant::Record(mut gr) => {
				// Token should not even be stored. We clear it just as a precaution.
				gr.token = None;
				Grant::Record(gr)
			}
			Grant::Bearer(mut gr) => {
				// Key is stored, but should not usually be displayed.
				gr.key = "[REDACTED]".into();
				Grant::Bearer(gr)
			}
		};
		ags
	}

	// Returns if the access grant is expired.
	pub fn is_expired(&self) -> bool {
		match &self.expiration {
			Some(exp) => exp < &Datetime::default(),
			None => false,
		}
	}

	// Returns if the access grant is revoked.
	pub fn is_revoked(&self) -> bool {
		self.revocation.is_some()
	}

	// Returns if the access grant is active.
	pub fn is_active(&self) -> bool {
		!(self.is_expired() || self.is_revoked())
	}
}

impl From<AccessGrant> for Object {
	fn from(grant: AccessGrant) -> Self {
		let mut res = Object::default();
		res.insert("id".to_owned(), SqlValue::from(grant.id.to_raw()));
		res.insert("ac".to_owned(), SqlValue::from(grant.ac.to_raw()));
		res.insert("type".to_owned(), SqlValue::from(grant.grant.variant()));
		res.insert("creation".to_owned(), SqlValue::from(grant.creation));
		res.insert("expiration".to_owned(), SqlValue::from(grant.expiration));
		res.insert("revocation".to_owned(), SqlValue::from(grant.revocation));
		let mut sub = Object::default();
		match grant.subject {
			Subject::Record(id) => sub.insert("record".to_owned(), SqlValue::from(id)),
			Subject::User(name) => sub.insert("user".to_owned(), SqlValue::from(name.to_raw())),
		};
		res.insert("subject".to_owned(), SqlValue::from(sub));

		let mut gr = Object::default();
		match grant.grant {
			Grant::Jwt(jg) => {
				gr.insert("jti".to_owned(), SqlValue::from(jg.jti));
				if let Some(token) = jg.token {
					gr.insert("token".to_owned(), SqlValue::from(token));
				}
			}
			Grant::Record(rg) => {
				gr.insert("rid".to_owned(), SqlValue::from(rg.rid));
				gr.insert("jti".to_owned(), SqlValue::from(rg.jti));
				if let Some(token) = rg.token {
					gr.insert("token".to_owned(), SqlValue::from(token));
				}
			}
			Grant::Bearer(bg) => {
				gr.insert("id".to_owned(), SqlValue::from(bg.id.to_raw()));
				gr.insert("key".to_owned(), SqlValue::from(bg.key));
			}
		};
		res.insert("grant".to_owned(), SqlValue::from(gr));

		res
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Subject {
	Record(Thing),
	User(Ident),
}

impl Subject {
	// Returns the main identifier of a subject as a string.
	pub fn id(&self) -> String {
		match self {
			Subject::Record(id) => id.to_raw(),
			Subject::User(name) => name.to_raw(),
		}
	}
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct GrantJwt {
	pub jti: Uuid,             // JWT ID
	pub token: Option<Strand>, // JWT. Will not be stored after being returned.
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct GrantRecord {
	pub rid: Uuid,             // Record ID
	pub jti: Uuid,             // JWT ID
	pub token: Option<Strand>, // JWT. Will not be stored after being returned.
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct GrantBearer {
	pub id: Ident, // Key ID
	// Key. Will not be stored and be returned as redacted.
	// Immediately after generation, it will contain the plaintext key.
	// Will be hashed before storage so that the plaintext key is not stored.
	pub key: Strand,
}

impl GrantBearer {
	pub fn new(prefix: &str) -> Self {
		let id = format!(
			"{}{}",
			// The pool for the first character of the key identifier excludes digits.
			random_string(1, &GRANT_BEARER_CHARACTER_POOL[10..]),
			random_string(GRANT_BEARER_ID_LENGTH - 1, GRANT_BEARER_CHARACTER_POOL)
		);
		let secret = random_string(GRANT_BEARER_KEY_LENGTH, GRANT_BEARER_CHARACTER_POOL);
		Self {
			id: id.clone().into(),
			key: format!("{prefix}-{id}-{secret}").into(),
		}
	}

	pub fn hashed(self) -> Self {
		// The hash of the bearer key is stored to mitigate the impact of a read-only compromise.
		// We use SHA-256 as the key needs to be verified performantly for every operation.
		// Unlike with passwords, brute force and rainbow tables are infeasable due to the key length.
		// When hashing the bearer keys, the prefix and key identifier are kept as salt.
		let mut hasher = Sha256::new();
		hasher.update(self.key.as_string());
		let hash = hasher.finalize();
		let hash_hex = format!("{hash:x}").into();

		Self {
			key: hash_hex,
			..self
		}
	}
}

fn random_string(length: usize, pool: &[u8]) -> String {
	let mut rng = rand::thread_rng();
	let string: String = (0..length)
		.map(|_| {
			let i = rng.gen_range(0..pool.len());
			pool[i] as char
		})
		.collect();
	string
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
				match stmt.subject {
					Subject::User(_) => write!(f, " FOR USER {}", stmt.subject.id())?,
					Subject::Record(_) => write!(f, " FOR RECORD {}", stmt.subject.id())?,
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
