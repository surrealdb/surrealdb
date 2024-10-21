use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::access_type::BearerAccessSubject;
use crate::sql::{
	AccessType, Array, Base, Cond, Datetime, Duration, Ident, Object, Strand, Thing, Uuid, Value,
};
use derive::Store;
use rand::Rng;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

pub static GRANT_BEARER_PREFIX: &str = "surreal-bearer";
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
	Show(AccessStatementShow),     // Show access grants.
	Revoke(AccessStatementRevoke), // Revoke access grant.
	Purge(AccessStatementPurge),   // Purge access grants.
}

// TODO(gguillemas): Document once bearer access is no longer experimental.
#[doc(hidden)]
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessStatementShow {
	pub ac: Ident,
	pub base: Option<Base>,
	pub gr: Option<Ident>,
	pub cond: Option<Cond>,
}

// TODO(gguillemas): Document once bearer access is no longer experimental.
#[doc(hidden)]
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessStatementRevoke {
	pub ac: Ident,
	pub base: Option<Base>,
	pub gr: Option<Ident>,
	pub cond: Option<Cond>,
}

// TODO(gguillemas): Document once bearer access is no longer experimental.
#[doc(hidden)]
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessStatementPurge {
	pub ac: Ident,
	pub base: Option<Base>,
	pub expired: bool,
	pub revoked: bool,
	pub grace: Duration,
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
		res.insert("id".to_owned(), Value::from(grant.id.to_raw()));
		res.insert("ac".to_owned(), Value::from(grant.ac.to_raw()));
		match grant.grant {
			Grant::Jwt(_) => res.insert("type".to_owned(), Value::from("jwt")),
			Grant::Record(_) => res.insert("type".to_owned(), Value::from("record")),
			Grant::Bearer(_) => res.insert("type".to_owned(), Value::from("bearer")),
		};
		res.insert("creation".to_owned(), Value::from(grant.creation));
		res.insert("expiration".to_owned(), Value::from(grant.expiration));
		res.insert("revocation".to_owned(), Value::from(grant.revocation));
		if let Some(subject) = grant.subject {
			let mut sub = Object::default();
			match subject {
				Subject::Record(id) => sub.insert("record".to_owned(), Value::from(id)),
				Subject::User(name) => sub.insert("user".to_owned(), Value::from(name.to_raw())),
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
	Record(Thing),
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
		let id = format!(
			"{}{}",
			// The pool for the first character of the key identifier excludes digits.
			random_string(1, &GRANT_BEARER_CHARACTER_POOL[10..]),
			random_string(GRANT_BEARER_ID_LENGTH - 1, GRANT_BEARER_CHARACTER_POOL)
		);
		let secret = random_string(GRANT_BEARER_KEY_LENGTH, GRANT_BEARER_CHARACTER_POOL);
		Self {
			id: id.clone().into(),
			key: format!("{GRANT_BEARER_PREFIX}-{id}-{secret}").into(),
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

async fn compute_grant(
	stmt: &AccessStatementGrant,
	ctx: &Context,
	opt: &Options,
	_doc: Option<&CursorDoc>,
) -> Result<Value, Error> {
	let base = match &stmt.base {
		Some(base) => base.clone(),
		None => opt.selected_base()?,
	};
	// Allowed to run?
	opt.is_allowed(Action::Edit, ResourceKind::Access, &base)?;
	// Get the transaction.
	let txn = ctx.tx();
	// Clear the cache.
	txn.clear();
	// Read the access definition.
	let ac = match base {
		Base::Root => txn.get_root_access(&stmt.ac).await?,
		Base::Ns => txn.get_ns_access(opt.ns()?, &stmt.ac).await?,
		Base::Db => txn.get_db_access(opt.ns()?, opt.db()?, &stmt.ac).await?,
		_ => {
			return Err(Error::Unimplemented(
				"Managing access methods outside of root, namespace and database levels"
					.to_string(),
			))
		}
	};
	// Verify the access type.
	match &ac.kind {
		AccessType::Jwt(_) => Err(Error::FeatureNotYetImplemented {
			feature: format!("Grants for JWT on {base}"),
		}),
		AccessType::Record(_) => Err(Error::FeatureNotYetImplemented {
			feature: format!("Grants for record on {base}"),
		}),
		AccessType::Bearer(at) => {
			match &stmt.subject {
				Some(Subject::User(user)) => {
					// Grant subject must match access method subject.
					if !matches!(&at.subject, BearerAccessSubject::User) {
						return Err(Error::AccessGrantInvalidSubject);
					}
					// If the grant is being created for a user, the user must exist.
					match base {
						Base::Root => txn.get_root_user(user).await?,
						Base::Ns => txn.get_ns_user(opt.ns()?, user).await?,
						Base::Db => txn.get_db_user(opt.ns()?, opt.db()?, user).await?,
						_ => return Err(Error::Unimplemented(
							"Managing access methods outside of root, namespace and database levels".to_string(),
						)),
					};
				}
				Some(Subject::Record(_)) => {
					// If the grant is being created for a record, a database must be selected.
					if !matches!(base, Base::Db) {
						return Err(Error::DbEmpty);
					}
					// Grant subject must match access method subject.
					if !matches!(&at.subject, BearerAccessSubject::Record) {
						return Err(Error::AccessGrantInvalidSubject);
					}
					// A grant can be created for a record that does not exist yet.
				}
				None => return Err(Error::AccessGrantInvalidSubject),
			};
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

			// Create the grant.
			// On the very unlikely event of a collision, "put" will return an error.
			let res = match base {
				Base::Root => {
					let key = crate::key::root::access::gr::new(&gr.ac, &gr.id);
					txn.put(key, &gr, None).await
				}
				Base::Ns => {
					let key = crate::key::namespace::access::gr::new(opt.ns()?, &gr.ac, &gr.id);
					txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
					txn.put(key, &gr, None).await
				}
				Base::Db => {
					let key =
						crate::key::database::access::gr::new(opt.ns()?, opt.db()?, &gr.ac, &gr.id);
					txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
					txn.get_or_add_db(opt.ns()?, opt.db()?, opt.strict).await?;
					txn.put(key, &gr, None).await
				}
				_ => {
					return Err(Error::Unimplemented(
						"Managing access methods outside of root, namespace and database levels"
							.to_string(),
					))
				}
			};

			// Check if a collision was found in order to log a specific error on the server.
			// For an access method with a billion grants, this chance is of only one in 295 billion.
			if let Err(Error::TxKeyAlreadyExists) = res {
				error!("A collision was found when attempting to create a new grant. Purging stale grants is advised")
			}
			res?;

			Ok(Value::Object(gr.into()))
		}
	}
}

async fn compute_show(
	stmt: &AccessStatementShow,
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	_doc: Option<&CursorDoc>,
) -> Result<Value, Error> {
	let base = match &stmt.base {
		Some(base) => base.clone(),
		None => opt.selected_base()?,
	};
	// Allowed to run?
	opt.is_allowed(Action::View, ResourceKind::Access, &base)?;
	// Get the transaction.
	let txn = ctx.tx();
	// Clear the cache.
	txn.clear();
	// Check if the access method exists.
	match base {
		Base::Root => txn.get_root_access(&stmt.ac).await?,
		Base::Ns => txn.get_ns_access(opt.ns()?, &stmt.ac).await?,
		Base::Db => txn.get_db_access(opt.ns()?, opt.db()?, &stmt.ac).await?,
		_ => {
			return Err(Error::Unimplemented(
				"Managing access methods outside of root, namespace and database levels"
					.to_string(),
			))
		}
	};

	// Get the grants to show.
	match &stmt.gr {
		Some(gr) => {
			let grant = match base {
				Base::Root => (*txn.get_root_access_grant(&stmt.ac, gr).await?).clone(),
				Base::Ns => (*txn.get_ns_access_grant(opt.ns()?, &stmt.ac, gr).await?).clone(),
				Base::Db => {
					(*txn.get_db_access_grant(opt.ns()?, opt.db()?, &stmt.ac, gr).await?).clone()
				}
				_ => {
					return Err(Error::Unimplemented(
						"Managing access methods outside of root, namespace and database levels"
							.to_string(),
					))
				}
			};

			Ok(Value::Object(grant.redacted().into()))
		}
		None => {
			// Get all grants.
			let grs =
				match base {
					Base::Root => txn.all_root_access_grants(&stmt.ac).await?,
					Base::Ns => txn.all_ns_access_grants(opt.ns()?, &stmt.ac).await?,
					Base::Db => txn.all_db_access_grants(opt.ns()?, opt.db()?, &stmt.ac).await?,
					_ => return Err(Error::Unimplemented(
						"Managing access methods outside of root, namespace and database levels"
							.to_string(),
					)),
				};

			let mut show = Vec::new();
			for gr in grs.iter() {
				// If provided, check if grant matches conditions.
				if let Some(cond) = &stmt.cond {
					// Redact grant before evaluating conditions.
					let redacted_gr = Value::Object(gr.redacted().to_owned().into());
					if !cond
						.compute(
							stk,
							ctx,
							opt,
							Some(&CursorDoc {
								rid: None,
								ir: None,
								doc: redacted_gr.into(),
							}),
						)
						.await?
						.is_truthy()
					{
						// Skip grant if it does not match the provided conditions.
						continue;
					}
				}

				// Store revoked version of the redacted grant.
				show.push(Value::Object(gr.redacted().to_owned().into()));
			}

			Ok(Value::Array(show.into()))
		}
	}
}

async fn compute_revoke(
	stmt: &AccessStatementRevoke,
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	_doc: Option<&CursorDoc>,
) -> Result<Value, Error> {
	let base = match &stmt.base {
		Some(base) => base.clone(),
		None => opt.selected_base()?,
	};
	// Allowed to run?
	opt.is_allowed(Action::Edit, ResourceKind::Access, &base)?;
	// Get the transaction
	let txn = ctx.tx();
	// Clear the cache
	txn.clear();
	// Check if the access method exists.
	match base {
		Base::Root => txn.get_root_access(&stmt.ac).await?,
		Base::Ns => txn.get_ns_access(opt.ns()?, &stmt.ac).await?,
		Base::Db => txn.get_db_access(opt.ns()?, opt.db()?, &stmt.ac).await?,
		_ => {
			return Err(Error::Unimplemented(
				"Managing access methods outside of root, namespace and database levels"
					.to_string(),
			))
		}
	};

	// Get the grants to revoke.
	match &stmt.gr {
		Some(gr) => {
			let mut revoked = match base {
				Base::Root => (*txn.get_root_access_grant(&stmt.ac, gr).await?).clone(),
				Base::Ns => (*txn.get_ns_access_grant(opt.ns()?, &stmt.ac, gr).await?).clone(),
				Base::Db => {
					(*txn.get_db_access_grant(opt.ns()?, opt.db()?, &stmt.ac, gr).await?).clone()
				}
				_ => {
					return Err(Error::Unimplemented(
						"Managing access methods outside of root, namespace and database levels"
							.to_string(),
					))
				}
			};
			if revoked.revocation.is_some() {
				return Err(Error::AccessGrantRevoked);
			}
			revoked.revocation = Some(Datetime::default());

			// Revoke the grant.
			match base {
				Base::Root => {
					let key = crate::key::root::access::gr::new(&stmt.ac, gr);
					txn.set(key, &revoked, None).await?;
				}
				Base::Ns => {
					let key = crate::key::namespace::access::gr::new(opt.ns()?, &stmt.ac, gr);
					txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
					txn.set(key, &revoked, None).await?;
				}
				Base::Db => {
					let key =
						crate::key::database::access::gr::new(opt.ns()?, opt.db()?, &stmt.ac, gr);
					txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
					txn.get_or_add_db(opt.ns()?, opt.db()?, opt.strict).await?;
					txn.set(key, &revoked, None).await?;
				}
				_ => {
					return Err(Error::Unimplemented(
						"Managing access methods outside of root, namespace and database levels"
							.to_string(),
					))
				}
			};

			Ok(Value::Object(revoked.redacted().into()))
		}
		None => {
			// Get all grants.
			let grs =
				match base {
					Base::Root => txn.all_root_access_grants(&stmt.ac).await?,
					Base::Ns => txn.all_ns_access_grants(opt.ns()?, &stmt.ac).await?,
					Base::Db => txn.all_db_access_grants(opt.ns()?, opt.db()?, &stmt.ac).await?,
					_ => return Err(Error::Unimplemented(
						"Managing access methods outside of root, namespace and database levels"
							.to_string(),
					)),
				};

			let mut revoked = Vec::new();
			for gr in grs.iter() {
				// If the grant is already revoked, it cannot be revoked again.
				if gr.revocation.is_some() {
					continue;
				}

				// If provided, check if grant matches conditions.
				if let Some(cond) = &stmt.cond {
					// Redact grant before evaluating conditions.
					let redacted_gr = Value::Object(gr.redacted().to_owned().into());
					if !cond
						.compute(
							stk,
							ctx,
							opt,
							Some(&CursorDoc {
								rid: None,
								ir: None,
								doc: redacted_gr.into(),
							}),
						)
						.await?
						.is_truthy()
					{
						// Skip grant if it does not match the provided conditions.
						continue;
					}
				}

				let mut gr = gr.clone();
				gr.revocation = Some(Datetime::default());

				// Revoke the grant.
				match base {
					Base::Root => {
						let key = crate::key::root::access::gr::new(&stmt.ac, &gr.id);
						txn.set(key, &gr, None).await?;
					}
					Base::Ns => {
						let key =
							crate::key::namespace::access::gr::new(opt.ns()?, &stmt.ac, &gr.id);
						txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
						txn.set(key, &gr, None).await?;
					}
					Base::Db => {
						let key = crate::key::database::access::gr::new(
							opt.ns()?,
							opt.db()?,
							&stmt.ac,
							&gr.id,
						);
						txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
						txn.get_or_add_db(opt.ns()?, opt.db()?, opt.strict).await?;
						txn.set(key, &gr, None).await?;
					}
					_ => return Err(Error::Unimplemented(
						"Managing access methods outside of root, namespace and database levels"
							.to_string(),
					)),
				};

				// Store revoked version of the redacted grant.
				revoked.push(Value::Object(gr.redacted().to_owned().into()));
			}

			// Return revoked grants.
			Ok(Value::Array(revoked.into()))
		}
	}
}

async fn compute_purge(
	stmt: &AccessStatementPurge,
	ctx: &Context,
	opt: &Options,
	_doc: Option<&CursorDoc>,
) -> Result<Value, Error> {
	let base = match &stmt.base {
		Some(base) => base.clone(),
		None => opt.selected_base()?,
	};
	// Allowed to run?
	opt.is_allowed(Action::Edit, ResourceKind::Access, &base)?;
	// Get the transaction.
	let txn = ctx.tx();
	// Clear the cache.
	txn.clear();
	// Check if the access method exists.
	match base {
		Base::Root => txn.get_root_access(&stmt.ac).await?,
		Base::Ns => txn.get_ns_access(opt.ns()?, &stmt.ac).await?,
		Base::Db => txn.get_db_access(opt.ns()?, opt.db()?, &stmt.ac).await?,
		_ => {
			return Err(Error::Unimplemented(
				"Managing access methods outside of root, namespace and database levels"
					.to_string(),
			))
		}
	};
	// Get all grants to purge.
	let mut purged = Array::default();
	let grs = match base {
		Base::Root => txn.all_root_access_grants(&stmt.ac).await?,
		Base::Ns => txn.all_ns_access_grants(opt.ns()?, &stmt.ac).await?,
		Base::Db => txn.all_db_access_grants(opt.ns()?, opt.db()?, &stmt.ac).await?,
		_ => {
			return Err(Error::Unimplemented(
				"Managing access methods outside of root, namespace and database levels"
					.to_string(),
			))
		}
	};
	for gr in grs.iter() {
		// Determine if the grant should purged based on expiration or revocation.
		let now = Datetime::default();
		// We can convert to unsigned integer as substraction is saturating.
		// Revocation times should never exceed the current time.
		// Grants expired or revoked at a future time will not be purged.
		// Grants expired or revoked at exactly the current second will not be purged.
		let purge_expired = stmt.expired
			&& gr.expiration.as_ref().map_or(false, |exp| {
				(now.timestamp().saturating_sub(exp.timestamp()) as u64) > stmt.grace.secs()
			});
		let purge_revoked = stmt.revoked
			&& gr.revocation.as_ref().map_or(false, |rev| {
				(now.timestamp().saturating_sub(rev.timestamp()) as u64) > stmt.grace.secs()
			});
		// If it should, delete the grant and append the redacted version to the result.
		if purge_expired || purge_revoked {
			match base {
				Base::Root => txn.del(crate::key::root::access::gr::new(&stmt.ac, &gr.id)).await?,
				Base::Ns => {
					txn.del(crate::key::namespace::access::gr::new(opt.ns()?, &stmt.ac, &gr.id))
						.await?
				}
				Base::Db => {
					txn.del(crate::key::database::access::gr::new(
						opt.ns()?,
						opt.db()?,
						&stmt.ac,
						&gr.id,
					))
					.await?
				}
				_ => {
					return Err(Error::Unimplemented(
						"Managing access methods outside of root, namespace and database levels"
							.to_string(),
					))
				}
			};
			purged = purged + Value::Object(gr.redacted().to_owned().into());
		}
	}

	Ok(Value::Array(purged))
}

impl AccessStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		match self {
			AccessStatement::Grant(stmt) => compute_grant(stmt, ctx, opt, _doc).await,
			AccessStatement::Show(stmt) => compute_show(stmt, stk, ctx, opt, _doc).await,
			AccessStatement::Revoke(stmt) => compute_revoke(stmt, stk, ctx, opt, _doc).await,
			AccessStatement::Purge(stmt) => compute_purge(stmt, ctx, opt, _doc).await,
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
				write!(f, " GRANT")?;
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
					(true, true) => write!(f, " ALL")?,
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
