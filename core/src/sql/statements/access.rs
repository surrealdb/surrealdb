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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum AccessStatement {
	Grant(AccessStatementGrant),   // Create access grant.
	List(AccessStatementList),     // List access grants.
	Revoke(AccessStatementRevoke), // Revoke access grant.
	Show(Ident),                   // Show access grant.
	Prune(Ident),                  // Prune access grants.
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessStatementList {
	pub ac: Ident,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessStatementGrant {
	pub ac: Ident,
	pub subject: Option<Subject>,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AccessStatementRevoke {
	pub ac: Ident,
	pub gr: Ident,
}

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
		let id = random_string(20);
		let secret = random_string(40);
		Self {
			id: id.clone().into(),
			key: format!("surreal-{id}-{secret}").into(),
		}
	}
}

fn random_string(length: usize) -> String {
	let charset: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	let mut rng = rand::thread_rng();
	let string: String = (0..length)
		.map(|_| {
			let i = rng.gen_range(0..charset.len());
			charset[i] as char
		})
		.collect();
	string
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
			AccessStatement::Grant(stmt) => {
				let base = opt.selected_base()?;
				// Allowed to run?
				opt.is_allowed(Action::Edit, ResourceKind::Access, &base)?;
				match base {
					Base::Ns => {
						// Claim transaction
						let mut run = ctx.tx_lock().await;
						// Clear the cache
						run.clear_cache();
						// Read the access definition
						let ac = run.get_ns_access(opt.ns()?, &stmt.ac.to_raw()).await?;
						// Verify the access type
						match ac.kind {
							AccessType::Jwt(_) => Err(Error::FeatureNotYetImplemented {
								feature: "Grants for JWT on namespace".to_string(),
							}),
							AccessType::Bearer(at) => {
								match &stmt.subject {
									Some(Subject::User(user)) => {
										// Grant subject must match access method level.
										if !matches!(&at.level, BearerAccessLevel::User) {
											// TODO(PR): Add new error.
											return Err(Error::InvalidAuth);
										}
										// If the grant is being created for a user, the user must exist.
										run.get_ns_user(opt.ns()?, user).await?;
									}
									Some(Subject::Record(_)) => {
										// If the grant is being created for a record, a database must be selected.
										// TODO(PR): Add new error.
										return Err(Error::DbEmpty);
									}
									// TODO(PR): Add new error.
									None => return Err(Error::InvalidAuth),
								}
								// Create a new bearer key.
								let grant = GrantBearer::new();
								let gr = AccessGrant {
									ac: ac.name,
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
								let key = crate::key::namespace::access::gr::new(
									opt.ns()?,
									&ac_str,
									&gr_str,
								);
								run.add_ns(opt.ns()?, opt.strict).await?;
								run.set(key, gr.to_owned()).await?;
								Ok(Value::Object(gr.into()))
							}
							_ => Err(Error::AccessMethodMismatch),
						}
					}
					Base::Db => {
						// Claim transaction
						let mut run = ctx.tx_lock().await;
						// Clear the cache
						run.clear_cache();
						// Read the access definition
						let ac = run.get_db_access(opt.ns()?, opt.db()?, &stmt.ac.to_raw()).await?;
						// Verify the access type
						match ac.kind {
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
											// TODO(PR): Add new error.
											return Err(Error::InvalidAuth);
										}
										// If the grant is being created for a user, the user must exist.
										run.get_db_user(opt.ns()?, opt.db()?, user).await?;
									}
									Some(Subject::Record(_)) => {
										// Grant subject must match access method level.
										if !matches!(&at.level, BearerAccessLevel::Record) {
											// TODO(PR): Add new error.
											return Err(Error::InvalidAuth);
										}
									}
									// TODO(PR): Add new error.
									None => return Err(Error::InvalidAuth),
								}
								// Create a new bearer key.
								let grant = GrantBearer::new();
								let gr = AccessGrant {
									ac: ac.name,
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
								run.add_ns(opt.ns()?, opt.strict).await?;
								run.add_db(opt.ns()?, opt.db()?, opt.strict).await?;
								run.set(key, gr.to_owned()).await?;
								Ok(Value::Object(gr.into()))
							}
						}
					}
					_ => Err(Error::FeatureNotYetImplemented {
						feature: "Managing access methods outside of a namespace or database"
							.to_string(),
					}),
				}
			}
			AccessStatement::List(stmt) => {
				let base = opt.selected_base()?;
				// Allowed to run?
				opt.is_allowed(Action::View, ResourceKind::Access, &base)?;
				match base {
					Base::Ns => {
						// Claim transaction
						let mut run = ctx.tx_lock().await;
						// Clear the cache
						run.clear_cache();
						// Get the grants for the access method
						let mut grants = Array::default();
						// TODO(PR): This should not return all data, only basic identifiers.
						for v in
							run.all_ns_access_grants_redacted(opt.ns()?, &stmt.ac).await?.iter()
						{
							grants = grants + Value::Object(v.to_owned().into());
						}
						Ok(Value::Array(grants))
					}
					Base::Db => {
						// Claim transaction
						let mut run = ctx.tx_lock().await;
						// Clear the cache
						run.clear_cache();
						// Get the grants for the access method
						let mut grants = Array::default();
						// TODO(PR): This should not return all data, only basic identifiers.
						for v in run
							.all_db_access_grants_redacted(opt.ns()?, opt.db()?, &stmt.ac)
							.await?
							.iter()
						{
							grants = grants + Value::Object(v.to_owned().into());
						}
						Ok(Value::Array(grants))
					}
					_ => Err(Error::FeatureNotYetImplemented {
						feature: "Managing access methods outside of a namespace or database"
							.to_string(),
					}),
				}
			}
			AccessStatement::Revoke(stmt) => {
				let base = opt.selected_base()?;
				// Allowed to run?
				opt.is_allowed(Action::Edit, ResourceKind::Access, &base)?;
				match base {
					Base::Ns => {
						// Claim transaction
						let mut run = ctx.tx_lock().await;
						// Clear the cache
						run.clear_cache();
						// Get the grants to revoke
						let ac_str = stmt.ac.to_raw();
						let gr_str = stmt.gr.to_raw();
						let mut gr = run.get_ns_access_grant(opt.ns()?, &ac_str, &gr_str).await?;
						if let Some(_) = gr.revocation {
							// TODO(PR): Add new error.
							return Err(Error::InvalidAuth);
						}
						gr.revocation = Some(Datetime::default());
						// Process the statement
						let key =
							crate::key::namespace::access::gr::new(opt.ns()?, &ac_str, &gr_str);
						run.add_ns(opt.ns()?, opt.strict).await?;
						run.set(key, gr.to_owned()).await?;
						Ok(Value::Object(gr.redacted().into()))
					}
					Base::Db => {
						// Claim transaction
						let mut run = ctx.tx_lock().await;
						// Clear the cache
						run.clear_cache();
						// Get the grants to revoke
						let ac_str = stmt.ac.to_raw();
						let gr_str = stmt.gr.to_raw();
						let mut gr =
							run.get_db_access_grant(opt.ns()?, opt.db()?, &ac_str, &gr_str).await?;
						if let Some(_) = gr.revocation {
							// TODO(PR): Add new error.
							return Err(Error::InvalidAuth);
						}
						gr.revocation = Some(Datetime::default());
						// Process the statement
						let key = crate::key::database::access::gr::new(
							opt.ns()?,
							opt.db()?,
							&ac_str,
							&gr_str,
						);
						run.add_ns(opt.ns()?, opt.strict).await?;
						run.add_db(opt.ns()?, opt.db()?, opt.strict).await?;
						run.set(key, gr.to_owned()).await?;
						Ok(Value::Object(gr.redacted().into()))
					}
					_ => Err(Error::FeatureNotYetImplemented {
						feature: "Managing access methods outside of a namespace or database"
							.to_string(),
					}),
				}
			}
			AccessStatement::Show(_) => Err(Error::FeatureNotYetImplemented {
				feature: "Showing an access grant".to_string(),
			}),
			AccessStatement::Prune(_) => Err(Error::FeatureNotYetImplemented {
				feature: "Pruning disabled grants".to_string(),
			}),
		}
	}
}

impl Display for AccessStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Grant(stmt) => write!(f, "ACCESS {} GRANT", stmt.ac),
			Self::List(stmt) => write!(f, "ACCESS {} LIST", stmt.ac),
			Self::Revoke(stmt) => write!(f, "ACCESS {} REVOKE {}", stmt.ac, stmt.gr),
			Self::Show(stmt) => write!(f, "ACCESS {} SHOW", stmt),
			Self::Prune(stmt) => write!(f, "ACCESS {} PRUNE", stmt),
		}
	}
}
