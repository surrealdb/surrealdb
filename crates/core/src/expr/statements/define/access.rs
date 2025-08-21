use std::fmt::{self, Display};

use anyhow::{Result, bail};
use rand::Rng;
use rand::distributions::Alphanumeric;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use super::DefineKind;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::access::AccessDuration;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{AccessType, Base, Expr, Ident};
use crate::iam::{Action, ResourceKind};
use crate::kvs::impl_kv_value_revisioned;
use crate::val::{Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct DefineAccessStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub base: Base,
	pub access_type: AccessType,
	pub authenticate: Option<Expr>,
	pub duration: AccessDuration,
	pub comment: Option<Strand>,
}

impl_kv_value_revisioned!(DefineAccessStatement);

impl DefineAccessStatement {
	/// Generate a random key to be used to sign session tokens
	/// This key will be used to sign tokens issued with this access method
	/// This value is used by default in every access method other than JWT
	pub(crate) fn random_key() -> String {
		rand::thread_rng().sample_iter(&Alphanumeric).take(128).map(char::from).collect::<String>()
	}

	/// Returns a version of the statement where potential secrets are redacted
	/// This function should be used when displaying the statement to datastore
	/// users This function should NOT be used when displaying the statement
	/// for export purposes
	pub fn redacted(&self) -> DefineAccessStatement {
		let mut das = self.clone();
		das.access_type = match das.access_type {
			AccessType::Jwt(ac) => AccessType::Jwt(ac.redacted()),
			AccessType::Record(mut ac) => {
				ac.jwt = ac.jwt.redacted();
				AccessType::Record(ac)
			}
			AccessType::Bearer(mut ac) => {
				ac.jwt = ac.jwt.redacted();
				AccessType::Bearer(ac)
			}
		};
		das
	}
}

impl DefineAccessStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;
		// Check the statement type
		match &self.base {
			Base::Root => {
				// Fetch the transaction
				let txn = ctx.tx();
				// Check if access method already exists
				if let Some(access) = txn.get_root_access(&self.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::AccessRootAlreadyExists {
									ac: access.name.to_string(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}
				// Process the statement
				let key = crate::key::root::ac::new(&self.name);
				txn.set(
					&key,
					&DefineAccessStatement {
						// Don't persist the `IF NOT EXISTS` clause to schema
						kind: DefineKind::Default,
						..self.clone()
					},
					None,
				)
				.await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Fetch the transaction
				let txn = ctx.tx();
				// Check if the definition exists
				let ns = ctx.get_ns_id(opt).await?;
				if let Some(access) = txn.get_ns_access(ns, &self.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::AccessNsAlreadyExists {
									ns: opt.ns()?.to_string(),
									ac: access.name.to_string(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}
				// Process the statement
				let key = crate::key::namespace::ac::new(ns, &self.name);
				txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
				txn.set(
					&key,
					&DefineAccessStatement {
						// Don't persist the `IF NOT EXISTS` clause to schema
						kind: DefineKind::Default,
						..self.clone()
					},
					None,
				)
				.await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Fetch the transaction
				let txn = ctx.tx();
				// Check if the definition exists
				let (ns, db) = ctx.get_ns_db_ids(opt).await?;
				if let Some(access) = txn.get_db_access(ns, db, &self.name).await? {
					match self.kind {
						DefineKind::Default => {
							if !opt.import {
								bail!(Error::AccessDbAlreadyExists {
									ns: opt.ns()?.to_string(),
									db: opt.db()?.to_string(),
									ac: access.name.to_string(),
								});
							}
						}
						DefineKind::Overwrite => {}
						DefineKind::IfNotExists => return Ok(Value::None),
					}
				}
				// Process the statement
				let key = crate::key::database::ac::new(ns, db, &self.name);
				txn.set(
					&key,
					&DefineAccessStatement {
						// Don't persist the `IF NOT EXISTS` clause to schema
						kind: DefineKind::Default,
						..self.clone()
					},
					None,
				)
				.await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			// Other levels are not supported
			_ => Err(anyhow::Error::new(Error::InvalidLevel(self.base.to_string()))),
		}
	}
}

impl Display for DefineAccessStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE ACCESS",)?;
		match self.kind {
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::Default => {}
		}
		// The specific access method definition is displayed by AccessType
		write!(f, " {} ON {} TYPE {}", self.name, self.base, self.access_type)?;
		// The additional authentication clause
		if let Some(ref v) = self.authenticate {
			write!(f, " AUTHENTICATE {v}")?
		}
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		if self.access_type.can_issue_grants() {
			write!(
				f,
				" FOR GRANT {},",
				match self.duration.grant {
					Some(dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
		if self.access_type.can_issue_tokens() {
			write!(
				f,
				" FOR TOKEN {},",
				match self.duration.token {
					Some(dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
		write!(
			f,
			" FOR SESSION {}",
			match self.duration.session {
				Some(dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineAccessStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"base".to_string() => self.base.structure(),
			"authenticate".to_string(), if let Some(v) = self.authenticate => v.structure(),
			"duration".to_string() => Value::from(map!{
				"session".to_string() => self.duration.session.map(Value::from).unwrap_or(Value::None),
				"grant".to_string(), if self.access_type.can_issue_grants() => self.duration.grant.map(Value::from).unwrap_or(Value::None),
				"token".to_string(), if self.access_type.can_issue_tokens() => self.duration.token.map(Value::from).unwrap_or(Value::None),
			}),
			"kind".to_string() => self.access_type.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
