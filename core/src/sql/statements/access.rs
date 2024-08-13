use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::access_type::BearerAccessLevel;
use crate::sql::{AccessType, Array, Base, Datetime, Id, Ident, Object, Strand, Uuid, Value};
use derive::Store;
use rand::Rng;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

pub static GRANT_BEARER_PREFIX: &str = "surreal-bearer";
// Keys and their identifiers are generated randomly from a 62-character pool.
pub static GRANT_BEARER_CHARACTER_POOL: &[u8] =
	b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
// The key identifier should not have collisions to prevent confusion.
// However, collisions should be handled gracefully when issuing grants.
// With 12 characters from the pool, the key identifier part has ~70 bits of entropy.
pub static GRANT_BEARER_ID_LENGTH: usize = 12;
// With 24 characters from the pool, the key part has ~140 bits of entropy.
pub static GRANT_BEARER_KEY_LENGTH: usize = 24;
// Total bearer key length.
pub static GRANT_BEARER_LENGTH: usize =
	GRANT_BEARER_PREFIX.len() + 1 + GRANT_BEARER_ID_LENGTH + 1 + GRANT_BEARER_KEY_LENGTH;

// TODO(gguillemas): Document once bearer access is no longer experimental.
#[doc(hidden)]
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum AccessStatement {
	Grant(AccessStatementGrant),   // Create access grant.
	List(AccessStatementList),     // List access grants.
	Revoke(AccessStatementRevoke), // Revoke access grant.
	Prune(Ident),                  // Prune access grants.
}

// TODO(gguillemas): Document once bearer access is no longer experimental.
#[doc(hidden)]
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessStatementList {
	pub ac: Ident,
	pub base: Option<Base>,
}

// TODO(gguillemas): Document once bearer access is no longer experimental.
#[doc(hidden)]
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessStatementGrant {
	pub ac: Ident,
	pub base: Option<Base>,
	pub subject: Option<Subject>,
}

// TODO(gguillemas): Document once bearer access is no longer experimental.
#[doc(hidden)]
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessStatementRevoke {
	pub ac: Ident,
	pub base: Option<Base>,
	pub gr: Ident,
}

// TODO(gguillemas): Document once bearer access is no longer experimental.
#[doc(hidden)]
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessGrant {
	pub id: Ident,                    // Unique grant identifier.
	pub ac: Ident,                    // Access method used to create the grant.
	pub creation: Datetime,           // Grant creation time.
	pub expiration: Option<Datetime>, // Grant expiration time, if any.
	pub revocation: Option<Datetime>, // Grant revocation time, if any.
	pub subject: Option<Subject>,     // Subject of the grant.
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
}

impl From<AccessGrant> for Object {
	fn from(grant: AccessGrant) -> Self {
		let mut res = Object::default();
		res.insert("id".to_owned(), Value::from(grant.id.to_raw()));
		res.insert("ac".to_owned(), Value::from(grant.ac.to_string()));
		res.insert("creation".to_owned(), Value::from(grant.creation));
		res.insert("expiration".to_owned(), Value::from(grant.expiration));
		res.insert("revocation".to_owned(), Value::from(grant.revocation));
		if let Some(subject) = grant.subject {
			let mut sub = Object::default();
			match subject {
				Subject::Record(id) => sub.insert("record".to_owned(), Value::from(id)),
				Subject::User(name) => sub.insert("user".to_owned(), Value::from(name.to_string())),
			};
			res.insert("subject".to_owned(), Value::from(sub));
		}

		let mut gr = Object::default();
		match grant.grant {
			Grant::Jwt(jg) => {
				gr.insert("jti".to_owned(), Value::from(jg.jti));
				if let Some(token) = jg.token {
					gr.insert("token".to_owned(), Value::from(token));
				}
			}
			Grant::Record(rg) => {
				gr.insert("rid".to_owned(), Value::from(rg.rid));
				gr.insert("jti".to_owned(), Value::from(rg.jti));
				if let Some(token) = rg.token {
					gr.insert("token".to_owned(), Value::from(token));
				}
			}
			Grant::Bearer(bg) => {
				gr.insert("id".to_owned(), Value::from(bg.id.to_raw()));
				gr.insert("key".to_owned(), Value::from(bg.key));
			}
		};
		res.insert("grant".to_owned(), Value::from(gr));

		res
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Subject {
	Record(Id),
	User(Ident),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Grant {
	Jwt(GrantJwt),
	Record(GrantRecord),
	Bearer(GrantBearer),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct GrantJwt {
	pub jti: Uuid,             // JWT ID
	pub token: Option<Strand>, // JWT. Will not be stored after being returned.
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct GrantRecord {
	pub rid: Uuid,             // Record ID
	pub jti: Uuid,             // JWT ID
	pub token: Option<Strand>, // JWT. Will not be stored after being returned.
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct GrantBearer {
	pub id: Ident,   // Key ID
	pub key: Strand, // Key. Will be stored but afterwards returned redacted.
}

impl GrantBearer {
	#[doc(hidden)]
	pub fn new() -> Self {
		let id = random_string(GRANT_BEARER_ID_LENGTH);
		let secret = random_string(GRANT_BEARER_KEY_LENGTH);
		Self {
			id: id.clone().into(),
			key: format!("{GRANT_BEARER_PREFIX}-{id}-{secret}").into(),
		}
	}
}

fn random_string(length: usize) -> String {
	let mut rng = rand::thread_rng();
	let string: String = (0..length)
		.map(|_| {
			let i = rng.gen_range(0..GRANT_BEARER_CHARACTER_POOL.len());
			GRANT_BEARER_CHARACTER_POOL[i] as char
		})
		.collect();
	string
}

async fn compute_grant(
	stmt: &AccessStatementGrant,
	ctx: &Context<'_>,
	opt: &Options,
	_doc: Option<&CursorDoc<'_>>,
) -> Result<Value, Error> {
	let base = match &stmt.base {
		Some(base) => base.clone(),
		None => opt.selected_base()?,
	};
	// Allowed to run?
	opt.is_allowed(Action::Edit, ResourceKind::Access, &base)?;
	match base {
		Base::Root => {
			// Get the transaction
			let txn = ctx.tx();
			// Clear the cache
			txn.clear();
			// Read the access definition
			let ac = txn.get_root_access(&stmt.ac.to_raw()).await?;
			// Verify the access type
			match &ac.kind {
				AccessType::Jwt(_) => Err(Error::FeatureNotYetImplemented {
					feature: "Grants for JWT on namespace".to_string(),
				}),
				AccessType::Bearer(at) => {
					match &stmt.subject {
						Some(Subject::User(user)) => {
							// Grant subject must match access method level.
							if !matches!(&at.level, BearerAccessLevel::User) {
								return Err(Error::AccessGrantInvalidSubject);
							}
							// If the grant is being created for a user, the user must exist.
							txn.get_root_user(user).await?;
						}
						Some(Subject::Record(_)) => {
							// If the grant is being created for a record, a database must be selected.
							return Err(Error::DbEmpty);
						}
						None => return Err(Error::AccessGrantInvalidSubject),
					}
					// Create a new bearer key.
					let grant = GrantBearer::new();
					let gr = AccessGrant {
						ac: ac.name.clone(),
						// Unique grant identifier.
						// In the case of bearer grants, the key identifier.
						id: grant.id.clone(),
						// Current time.
						creation: Datetime::default(),
						// Current time plus grant duration. Only if set.
						expiration: ac.duration.grant.map(|d| d + Datetime::default()),
						// The grant is initially not revoked.
						revocation: None,
						// Subject associated with the grant.
						subject: stmt.subject.to_owned(),
						// The contents of the grant.
						grant: Grant::Bearer(grant),
					};
					let ac_str = gr.ac.to_raw();
					let gr_str = gr.id.to_raw();
					// Process the statement
					let key = crate::key::root::access::gr::new(&ac_str, &gr_str);
					txn.set(key, &gr).await?;
					Ok(Value::Object(gr.into()))
				}
				_ => Err(Error::AccessMethodMismatch),
			}
		}
		Base::Ns => {
			// Get the transaction
			let txn = ctx.tx();
			// Clear the cache
			txn.clear();
			// Read the access definition
			let ac = txn.get_ns_access(opt.ns()?, &stmt.ac.to_raw()).await?;
			// Verify the access type
			match &ac.kind {
				AccessType::Jwt(_) => Err(Error::FeatureNotYetImplemented {
					feature: "Grants for JWT on namespace".to_string(),
				}),
				AccessType::Bearer(at) => {
					match &stmt.subject {
						Some(Subject::User(user)) => {
							// Grant subject must match access method level.
							if !matches!(&at.level, BearerAccessLevel::User) {
								return Err(Error::AccessGrantInvalidSubject);
							}
							// If the grant is being created for a user, the user must exist.
							txn.get_ns_user(opt.ns()?, user).await?;
						}
						Some(Subject::Record(_)) => {
							// If the grant is being created for a record, a database must be selected.
							return Err(Error::DbEmpty);
						}
						None => return Err(Error::AccessGrantInvalidSubject),
					}
					// Create a new bearer key.
					let grant = GrantBearer::new();
					let gr = AccessGrant {
						ac: ac.name.clone(),
						// Unique grant identifier.
						// In the case of bearer grants, the key identifier.
						id: grant.id.clone(),
						// Current time.
						creation: Datetime::default(),
						// Current time plus grant duration. Only if set.
						expiration: ac.duration.grant.map(|d| d + Datetime::default()),
						// The grant is initially not revoked.
						revocation: None,
						// Subject associated with the grant.
						subject: stmt.subject.to_owned(),
						// The contents of the grant.
						grant: Grant::Bearer(grant),
					};
					let ac_str = gr.ac.to_raw();
					let gr_str = gr.id.to_raw();
					// Process the statement
					let key = crate::key::namespace::access::gr::new(opt.ns()?, &ac_str, &gr_str);
					txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
					txn.set(key, &gr).await?;
					Ok(Value::Object(gr.into()))
				}
				_ => Err(Error::AccessMethodMismatch),
			}
		}
		Base::Db => {
			// Get the transaction
			let txn = ctx.tx();
			// Clear the cache
			txn.clear();
			// Read the access definition
			let ac = txn.get_db_access(opt.ns()?, opt.db()?, &stmt.ac.to_raw()).await?;
			// Verify the access type
			match &ac.kind {
				AccessType::Jwt(_) => Err(Error::FeatureNotYetImplemented {
					feature: "Grants for JWT on database".to_string(),
				}),
				AccessType::Record(_) => Err(Error::FeatureNotYetImplemented {
					feature: "Grants for record on database".to_string(),
				}),
				AccessType::Bearer(at) => {
					match &stmt.subject {
						Some(Subject::User(user)) => {
							// Grant subject must match access method level.
							if !matches!(&at.level, BearerAccessLevel::User) {
								return Err(Error::AccessGrantInvalidSubject);
							}
							// If the grant is being created for a user, the user must exist.
							txn.get_db_user(opt.ns()?, opt.db()?, user).await?;
						}
						Some(Subject::Record(_)) => {
							// Grant subject must match access method level.
							if !matches!(&at.level, BearerAccessLevel::Record) {
								return Err(Error::AccessGrantInvalidSubject);
							}
						}
						None => return Err(Error::AccessGrantInvalidSubject),
					}
					// Create a new bearer key.
					let grant = GrantBearer::new();
					let gr = AccessGrant {
						ac: ac.name.clone(),
						// Unique grant identifier.
						// In the case of bearer grants, the key identifier.
						id: grant.id.clone(),
						// Current time.
						creation: Datetime::default(),
						// Current time plus grant duration. Only if set.
						expiration: ac.duration.grant.map(|d| d + Datetime::default()),
						// The grant is initially not revoked.
						revocation: None,
						// Subject associated with the grant.
						subject: stmt.subject.clone(),
						// The contents of the grant.
						grant: Grant::Bearer(grant),
					};
					let ac_str = gr.ac.to_raw();
					let gr_str = gr.id.to_raw();
					// Process the statement
					let key = crate::key::database::access::gr::new(
						opt.ns()?,
						opt.db()?,
						&ac_str,
						&gr_str,
					);
					txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
					txn.get_or_add_db(opt.ns()?, opt.db()?, opt.strict).await?;
					txn.set(key, &gr).await?;
					Ok(Value::Object(gr.into()))
				}
			}
		}
		_ => Err(Error::Unimplemented(
			"Managing access methods outside of root, namespace and database levels".to_string(),
		)),
	}
}

async fn compute_list(
	stmt: &AccessStatementList,
	ctx: &Context<'_>,
	opt: &Options,
	_doc: Option<&CursorDoc<'_>>,
) -> Result<Value, Error> {
	let base = match &stmt.base {
		Some(base) => base.clone(),
		None => opt.selected_base()?,
	};
	// Allowed to run?
	opt.is_allowed(Action::View, ResourceKind::Access, &base)?;
	match base {
		Base::Root => {
			// Get the transaction
			let txn = ctx.tx();
			// Clear the cache
			txn.clear();
			// Check if the access method exists.
			txn.get_root_access(&stmt.ac).await?;
			// Get the grants for the access method.
			let mut grants = Array::default();
			// Show redacted version of the access grants.
			for v in txn.all_root_access_grants(&stmt.ac).await?.iter() {
				grants = grants + Value::Object(v.redacted().to_owned().into());
			}
			Ok(Value::Array(grants))
		}
		Base::Ns => {
			// Get the transaction
			let txn = ctx.tx();
			// Clear the cache
			txn.clear();
			// Check if the access method exists.
			txn.get_ns_access(opt.ns()?, &stmt.ac).await?;
			// Get the grants for the access method.
			let mut grants = Array::default();
			// Show redacted version of the access grants.
			for v in txn.all_ns_access_grants(opt.ns()?, &stmt.ac).await?.iter() {
				grants = grants + Value::Object(v.redacted().to_owned().into());
			}
			Ok(Value::Array(grants))
		}
		Base::Db => {
			// Get the transaction
			let txn = ctx.tx();
			// Clear the cache
			txn.clear();
			// Check if the access method exists.
			txn.get_db_access(opt.ns()?, opt.db()?, &stmt.ac).await?;
			// Get the grants for the access method.
			let mut grants = Array::default();
			// Show redacted version of the access grants.
			for v in txn.all_db_access_grants(opt.ns()?, opt.db()?, &stmt.ac).await?.iter() {
				grants = grants + Value::Object(v.redacted().to_owned().into());
			}
			Ok(Value::Array(grants))
		}
		_ => Err(Error::Unimplemented(
			"Managing access methods outside of root, namespace and database levels".to_string(),
		)),
	}
}

async fn compute_revoke(
	stmt: &AccessStatementRevoke,
	ctx: &Context<'_>,
	opt: &Options,
	_doc: Option<&CursorDoc<'_>>,
) -> Result<Value, Error> {
	let base = match &stmt.base {
		Some(base) => base.clone(),
		None => opt.selected_base()?,
	};
	// Allowed to run?
	opt.is_allowed(Action::Edit, ResourceKind::Access, &base)?;
	match base {
		Base::Root => {
			// Get the transaction
			let txn = ctx.tx();
			// Clear the cache
			txn.clear();
			// Check if the access method exists.
			txn.get_root_access(&stmt.ac).await?;
			// Get the grants to revoke
			let ac_str = stmt.ac.to_raw();
			let gr_str = stmt.gr.to_raw();
			let mut gr = (*txn.get_root_access_grant(&ac_str, &gr_str).await?).clone();
			if gr.revocation.is_some() {
				return Err(Error::AccessGrantRevoked);
			}
			gr.revocation = Some(Datetime::default());
			// Process the statement
			let key = crate::key::root::access::gr::new(&ac_str, &gr_str);
			txn.set(key, &gr).await?;
			Ok(Value::Object(gr.redacted().into()))
		}
		Base::Ns => {
			// Get the transaction
			let txn = ctx.tx();
			// Clear the cache
			txn.clear();
			// Check if the access method exists.
			txn.get_ns_access(opt.ns()?, &stmt.ac).await?;
			// Get the grants to revoke
			let ac_str = stmt.ac.to_raw();
			let gr_str = stmt.gr.to_raw();
			let mut gr = (*txn.get_ns_access_grant(opt.ns()?, &ac_str, &gr_str).await?).clone();
			if gr.revocation.is_some() {
				return Err(Error::AccessGrantRevoked);
			}
			gr.revocation = Some(Datetime::default());
			// Process the statement
			let key = crate::key::namespace::access::gr::new(opt.ns()?, &ac_str, &gr_str);
			txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
			txn.set(key, &gr).await?;
			Ok(Value::Object(gr.redacted().into()))
		}
		Base::Db => {
			// Get the transaction
			let txn = ctx.tx();
			// Clear the cache
			txn.clear();
			// Check if the access method exists.
			txn.get_db_access(opt.ns()?, opt.db()?, &stmt.ac).await?;
			// Get the grants to revoke
			let ac_str = stmt.ac.to_raw();
			let gr_str = stmt.gr.to_raw();
			let mut gr =
				(*txn.get_db_access_grant(opt.ns()?, opt.db()?, &ac_str, &gr_str).await?).clone();
			if gr.revocation.is_some() {
				return Err(Error::AccessGrantRevoked);
			}
			gr.revocation = Some(Datetime::default());
			// Process the statement
			let key = crate::key::database::access::gr::new(opt.ns()?, opt.db()?, &ac_str, &gr_str);
			txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
			txn.get_or_add_db(opt.ns()?, opt.db()?, opt.strict).await?;
			txn.set(key, &gr).await?;
			Ok(Value::Object(gr.redacted().into()))
		}
		_ => Err(Error::Unimplemented(
			"Managing access methods outside of root, namespace and database levels".to_string(),
		)),
	}
}

impl AccessStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match self {
			AccessStatement::Grant(stmt) => compute_grant(stmt, ctx, opt, _doc).await,
			AccessStatement::List(stmt) => compute_list(stmt, ctx, opt, _doc).await,
			AccessStatement::Revoke(stmt) => compute_revoke(stmt, ctx, opt, _doc).await,
			AccessStatement::Prune(_) => Err(Error::FeatureNotYetImplemented {
				feature: "Pruning disabled grants".to_string(),
			}),
		}
	}
}

impl Display for AccessStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Grant(stmt) => {
				write!(f, "ACCESS {}", stmt.ac)?;
				if let Some(ref v) = stmt.base {
					write!(f, " ON {v}")?;
				}
				write!(f, "GRANT")?;
				Ok(())
			}
			Self::List(stmt) => {
				write!(f, "ACCESS {}", stmt.ac)?;
				if let Some(ref v) = stmt.base {
					write!(f, " ON {v}")?;
				}
				write!(f, "LIST")?;
				Ok(())
			}
			Self::Revoke(stmt) => {
				write!(f, "ACCESS {}", stmt.ac)?;
				if let Some(ref v) = stmt.base {
					write!(f, " ON {v}")?;
				}
				write!(f, "REVOKE {}", stmt.gr)?;
				Ok(())
			}
			Self::Prune(stmt) => write!(f, "ACCESS {} PRUNE", stmt),
		}
	}
}
